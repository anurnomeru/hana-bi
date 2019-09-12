package com.anur.core.coordinate.model

import com.anur.config.InetSocketAddressConfigHelper

class HanabiNode(val serverName: String, val host: String, val electionPort: Int, val coordinatePort: Int) {

    companion object {
        val NOT_EXIST = HanabiNode("", "", 0, 0)
    }

    /**
     * 是否是本地节点
     */
    fun isLocalNode(): Boolean {
        return this.serverName == InetSocketAddressConfigHelper.getServerName()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as HanabiNode

        if (serverName != other.serverName) return false
        if (host != other.host) return false
        if (electionPort != other.electionPort) return false
        if (coordinatePort != other.coordinatePort) return false
        return true
    }

    override fun hashCode(): Int {
        var result = serverName.hashCode()
        result = 31 * result + host.hashCode()
        result = 31 * result + electionPort
        result = 31 * result + coordinatePort
        return result
    }
}