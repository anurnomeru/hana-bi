package ink.anur.core.base

import java.util.Objects

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 *
 * Hanabi 节点，保存了初始化连接另一个客户端需要哪个地址、哪些端口
 */
class HanabiLegal(val serverName: String, val host: String, val electionPort: Int, val coordinatePort: Int) {

    companion object {
        val NOT_EXIST = HanabiLegal("", "", 0, 0)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val that = other as HanabiLegal?
        return serverName == that!!.serverName
    }

    override fun hashCode(): Int {
        return Objects.hash(serverName)
    }
}