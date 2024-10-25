package ar.edu.austral.inf.sd.server.model

import java.util.*

data class Node(
    val host: String,
    val port: Int,
    val uuid: UUID,
    val salt: String,
    val name: String
)