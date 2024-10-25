package ar.edu.austral.inf.sd

import ar.edu.austral.inf.sd.server.api.*
import ar.edu.austral.inf.sd.server.model.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.getAndUpdate
import kotlinx.coroutines.flow.update
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Component
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import org.springframework.web.client.RestClientException
import org.springframework.web.client.RestTemplate
import org.springframework.web.client.postForEntity
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import java.security.MessageDigest
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random

@Component
class ApiServicesImpl @Autowired constructor(
    private val restTemplate: RestTemplate
) : RegisterNodeApiService, RelayApiService, PlayApiService, UnregisterNodeApiService,
    ReconfigureApiService {

    @Value("\${server.name:nada}")
    private val myServerName: String = ""

    @Value("\${server.port:8085}")
    private val myServerPort: Int = 0

    @Value("\${server.host:localhost}")
    private val myServerHost: String = "localhost"

    @Value("\${server.timeout:20}")
    private val timeout: Int = 20

    @Value("\${register.host:}")
    var registerHost: String = ""

    @Value("\${register.port:-1}")
    var registerPort: Int = -1


    private val myUUID = UUID.randomUUID()
    private val mySalt = Base64.getEncoder().encodeToString(Random.nextBytes(9))

    private val playerNodes: MutableList<Node> = mutableListOf()
    private var nextNode: RegisterResponse? = null
    private val messageDigest = MessageDigest.getInstance("SHA-512")
    private val salt = Base64.getEncoder().encodeToString(Random.nextBytes(9))
    private val currentRequest
        get() = (RequestContextHolder.getRequestAttributes() as ServletRequestAttributes).request
    private var resultReady = CountDownLatch(1)
    private var currentMessageWaiting = MutableStateFlow<PlayResponse?>(null)
    private var currentMessageResponse = MutableStateFlow<PlayResponse?>(null)
    private var xGameTimestamp: Int = 0
    private var timeoutsCounter: Int = 0

    private var reconfiguredNode: RegisterResponse? = null

    override fun registerNode(
        host: String?,
        port: Int?,
        uuid: UUID?,
        salt: String?,
        name: String?
    ): ResponseEntity<RegisterResponse> {

        val actualNode = playerNodes.find { it.uuid == uuid }
        if (actualNode != null) {
            if (actualNode.salt != salt) {
                throw BadRequestException("Salt is incorrect")
            }
            val nextNodeIndex = playerNodes.indexOf(actualNode) - 1
            val nextNode = playerNodes[nextNodeIndex]
            return ResponseEntity(
                RegisterResponse(nextNode.host, nextNode.port, timeout, xGameTimestamp),
                HttpStatus.ACCEPTED
            )
        }

        val nextNode = if (playerNodes.isEmpty()) {
            // es el primer nodo
            val node = Node(myServerHost, myServerPort, myUUID, mySalt, myServerName)
            val response = RegisterResponse(myServerHost, myServerPort, timeout, xGameTimestamp)
            playerNodes.add(node)
            response
        } else {
            val lastPlayerNode = playerNodes.last()
            RegisterResponse(lastPlayerNode.host, lastPlayerNode.port, timeout, xGameTimestamp)
        }
        val node = Node(host!!, port!!, uuid!!, salt!!, name!!)
        playerNodes.add(node)

        return ResponseEntity(
            RegisterResponse(nextNode.nextHost, nextNode.nextPort, timeout, xGameTimestamp),
            HttpStatus.ACCEPTED
        )
    }

    override fun relayMessage(message: String, signatures: Signatures, xGameTimestamp: Int?): Signature {
        val receivedHash = doHash(message.encodeToByteArray(), mySalt)
        val receivedContentType = currentRequest.getPart("message")?.contentType ?: "nada"
        val receivedLength = message.length
        if (nextNode != null) {
            // Soy un relé. busco el siguiente y lo mando
            val signature = clientSign(message, receivedContentType)
            val updatedSignatures = Signatures(signatures.items + signature)
            sendRelayMessage(message, receivedContentType, nextNode!!, updatedSignatures, xGameTimestamp!!)
        } else {
            // me llego algo, no lo tengo que pasar
            if (currentMessageWaiting.value == null) throw BadRequestException("no waiting message")
            val current = currentMessageWaiting.getAndUpdate { null }!!
            val response = current.copy(
                contentResult = if (receivedHash == current.originalHash) "Success" else "Failure",
                receivedHash = receivedHash,
                receivedLength = receivedLength,
                receivedContentType = receivedContentType,
                signatures = signatures
            )
            currentMessageResponse.update { response }
            this.xGameTimestamp += 1
            resultReady.countDown()
        }
        return Signature(
            name = myServerName,
            hash = receivedHash,
            contentType = receivedContentType,
            contentLength = receivedLength
        )
    }

    override fun sendMessage(body: String): PlayResponse {
        if (timeoutsCounter >= 10) {
            throw BadRequestException("Timeouts exceeded")
        }
        if (playerNodes.isEmpty()) {
            // inicializamos el primer nodo como yo mismo
            val me = Node(myServerName, myServerPort, myUUID, mySalt, myServerName)
            playerNodes.add(me)
        }
        currentMessageWaiting.update { newResponse(body) }
        val contentType = currentRequest.contentType
        val signatures = generateSignatures(body, playerNodes, contentType)
        val lastNode = playerNodes.last()
        val registerResponse = RegisterResponse(lastNode.host, lastNode.port, timeout, xGameTimestamp + 1)
        sendRelayMessage(body, contentType, registerResponse, signatures, xGameTimestamp + 1)
        resultReady.await(timeout.toLong(), TimeUnit.SECONDS)
        resultReady = CountDownLatch(1)

        if (currentMessageWaiting.value == null) {
            timeoutsCounter += 1
            throw TimeOutException("Last message was not received on time")
        }
        if (doHash(body.encodeToByteArray(), mySalt) != currentMessageResponse.value!!.receivedHash) {
            throw ServiceUnavailableException("Received different hash than original")
        }
        return currentMessageResponse.value!!
    }

    override fun unregisterNode(uuid: UUID?, salt: String?): String {
        val node = playerNodes.find { it.uuid == uuid  && it.salt == salt}
        if (node == null) {
            throw BadRequestException("Invalid UUID or salt")
        }
        val index = playerNodes.indexOf(node)
        if (index < playerNodes.size - 1){
            val previousNode = playerNodes[index+1]
            val nextNode = playerNodes[index-1]

            val reconfigureUrl = "http://${previousNode.host}:${previousNode.port}/reconfigure"
            val reconfigureParams = "?uuid=${previousNode.uuid}&salt=${previousNode.salt}&nextHost=${nextNode.host}&nextPort=${nextNode.port}"

            val url = reconfigureUrl + reconfigureParams

            val requestHeaders = HttpHeaders().apply {
                add("X-Game-Timestamp", xGameTimestamp.toString())
            }
            val request = HttpEntity(null, requestHeaders)

            try {
                restTemplate.postForEntity<String>(url, request)
            } catch (e: RestClientException){
                throw e
            }
        }
        playerNodes.removeAt(index)
        return "Node unregistered"
    }

    override fun reconfigure(
        uuid: UUID?,
        salt: String?,
        nextHost: String?,
        nextPort: Int?,
        xGameTimestamp: Int?
    ): String {
        if (uuid != myUUID || salt != mySalt) {
            throw BadRequestException("Invalid UUID or salt")
        }
        reconfiguredNode = RegisterResponse(nextHost!!, nextPort!!, timeout, xGameTimestamp!!)
        return "OK"

    }

    internal fun registerToServer(registerHost: String, registerPort: Int) {
        val registerUrl =
            "http://$registerHost:$registerPort/register-node?host=localhost&port=$myServerPort&name=$myServerName&uuid=$myUUID&salt=$mySalt&name=$myServerName"
        try {
            val response = restTemplate.postForEntity<RegisterResponse>(registerUrl)
            val registerNodeResponse: RegisterResponse = response.body!!
            println("nextNode = $registerNodeResponse")
            nextNode = with(registerNodeResponse) { RegisterResponse(nextHost, nextPort, timeout, xGameTimestamp) }
            println(playerNodes)
        } catch (e: RestClientException) {
            println("Failed to register to server $registerHost:$registerPort :${e.message}")
        }
    }

    private fun sendRelayMessage(
        body: String,
        contentType: String,
        relayNode: RegisterResponse,
        signatures: Signatures,
        playerTimestamp: Int,
    ) {
        if (playerTimestamp < this.xGameTimestamp) {
            throw BadRequestException("Invalid timestamp")
        }
        val nextNodeUrl: String
        if (reconfiguredNode != null && playerTimestamp >= reconfiguredNode!!.xGameTimestamp) {
            xGameTimestamp = reconfiguredNode!!.xGameTimestamp
            nextNode = reconfiguredNode
            reconfiguredNode = null
            nextNodeUrl = "http://${nextNode!!.nextHost}:${nextNode!!.nextPort}/relay"
        } else {
            nextNodeUrl = "http://${relayNode.nextHost}:${relayNode.nextPort}/relay"
        }
        val headers = HttpHeaders().apply {
            setContentType(MediaType.MULTIPART_FORM_DATA)
            set("X-Game-Timestamp", xGameTimestamp.toString())
        }

        val messageHeaders = HttpHeaders().apply { setContentType(MediaType.parseMediaType(contentType)) }
        val messagePart = HttpEntity(body, messageHeaders)

        val signatureHeaders = HttpHeaders().apply { setContentType(MediaType.APPLICATION_JSON) }
        val signaturesPart = HttpEntity(signatures, signatureHeaders)

        val relayBody: MultiValueMap<String, Any> = LinkedMultiValueMap()
        relayBody.add("message", messagePart)
        relayBody.add("signatures", signaturesPart)

        val requestEntity = HttpEntity(relayBody, headers)

        try {
            val response = restTemplate.postForEntity(
                nextNodeUrl,
                requestEntity,
                String::class.java
            )
            println("Message relayed successfully to ${relayNode.nextHost}:${relayNode.nextPort}. Response: ${response.body}")
        } catch (e: RestClientException) {
            println("Failed to relay message to ${relayNode.nextHost}:${relayNode.nextPort}: ${e.message}")
            throw e
        }

    }

    private fun clientSign(message: String, contentType: String): Signature {
        val receivedHash = doHash(message.encodeToByteArray(), salt)
        return Signature(myServerName, receivedHash, contentType, message.length)
    }

    private fun newResponse(body: String) = PlayResponse(
        "Unknown",
        currentRequest.contentType,
        body.length,
        doHash(body.encodeToByteArray(), salt),
        "Unknown",
        -1,
        "N/A",
        Signatures(listOf())
    )

    private fun doHash(body: ByteArray, salt: String): String {
        val saltBytes = Base64.getDecoder().decode(salt)
        messageDigest.update(saltBytes)
        val digest = messageDigest.digest(body)
        return Base64.getEncoder().encodeToString(digest)
    }

    private fun generateSignatures(body: String, nodes: List<Node>, contentType: String): Signatures {
        val signatures = mutableListOf<Signature>()
        for (i in 1..nodes.size) {
            val node = nodes[i]
            val signature = Signature(node.name, doHash(body.encodeToByteArray(), node.salt), contentType, body.length)
            signatures.add(signature)
        }
        return Signatures(signatures)
    }

    companion object {
        fun newSalt(): String = Base64.getEncoder().encodeToString(Random.nextBytes(9))
    }
}