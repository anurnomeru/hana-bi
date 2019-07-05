package ink.anur.config

import com.anur.core.util.ChannelManager
import com.anur.exception.HanabiException
import ink.anur.config.common.ConfigHelper
import ink.anur.config.common.ConfigurationEnum
import ink.anur.core.base.HanabiLegal

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 *
 * 网络相关配置，都可以从这里获取
 */
object InetSocketAddressConfiguration : ConfigHelper() {

    private val me: HanabiLegal

    init {
        val name = getConfig(ConfigurationEnum.SERVER_NAME) { unChange -> unChange } as String
        if (name == ChannelManager.CoordinateLeaderSign) {
            throw HanabiException(" 'Leader' 为关键词，节点不能命名为这个")
        }
        me = getNode(name)
    }

    fun getServerElectionPort(): Int {
        return me.electionPort
    }

    fun getServerCoordinatePort(): Int {
        return me.coordinatePort
    }

    fun getCluster(): List<HanabiLegal> {
        return getConfigSimilar(ConfigurationEnum.CLIENT_ADDR) { pair ->
            val serverName = pair.key
            val split = pair.value
                .split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            HanabiLegal(serverName, split[0], Integer.valueOf(split[1]), Integer.valueOf(split[2]))
        } as List<HanabiLegal>
    }

    fun getNode(serverName: String): HanabiLegal {
        return getCluster().associateBy { hanabiLegal: HanabiLegal -> hanabiLegal.serverName }[serverName] ?: HanabiLegal.NOT_EXIST
    }

}
