package ink.anur.config.common

import com.anur.exception.HanabiException
import javafx.util.Pair
import java.util.ResourceBundle
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 *
 * 统一操作配置文件入口
 */
open class ConfigHelper {

    companion object {
        @Volatile
        private var resourceBundle: ResourceBundle = ResourceBundle.getBundle("application")
        private const val errorFormatter = "读取application.properties配置异常，异常项目：%s，建议：%s"
    }


    private val readLock: Lock
    private val writeLock: Lock
    private val cache = ConcurrentHashMap<ConfigEnum, Any>()


    init {
        val readWriteLock = ReentrantReadWriteLock()
        readLock = readWriteLock.readLock()
        writeLock = readWriteLock.writeLock()
    }

    /**
     * 优先获取缓存中的值，如果获取不到再从配置文件获取
     */
    private fun lockSupplier(configEnum: ConfigEnum, supplier: () -> Any): Any {
        val t: Any
        try {
            readLock.lock()
            cache.containsKey(configEnum)

            (if (cache.containsKey(configEnum)) {
                t = cache[configEnum]!!
            } else {
                t = supplier.invoke()
                cache[configEnum] = t
            })

        } finally {
            readLock.unlock()
        }
        return t
    }

    /**
     * 刷新配置
     */
    fun refresh() {
        try {
            writeLock.lock()
            cache.clear()
            resourceBundle = ResourceBundle.getBundle("application")
        } finally {
            writeLock.unlock()
        }
    }

    /**
     * 根据key获取某个配置
     */
    internal fun getConfig(configEnum: ConfigEnum, transfer: (String) -> Any?): Any {
        return lockSupplier(configEnum) {
            transfer.invoke(resourceBundle.getString(configEnum.key))
                ?: throw ApplicationConfigException(String.format(errorFormatter, configEnum.key, configEnum.adv))
        }
    }

    /**
     * 根据key模糊得获取某些配置，匹配规则为 key%
     */
    internal fun getConfigSimilar(configEnum: ConfigEnum, transfer: (Pair<String, String>) -> Any?): Any {
        return lockSupplier(configEnum) {
            val stringEnumeration = resourceBundle.keys
            val keys = mutableListOf<String>()

            while (stringEnumeration.hasMoreElements()) {
                val k = stringEnumeration.nextElement()
                if (k.startsWith(configEnum.key)) {
                    keys.add(k)
                }
            }

            keys.map {
                transfer.invoke(
                    Pair(
                        if (it.length > configEnum.key.length) it.substring(configEnum.key.length + 1) else it,
                        resourceBundle.getString(it)
                    )) ?: throw ApplicationConfigException(String.format(errorFormatter, configEnum.key, configEnum.adv))
            }
        }
    }
}

class ApplicationConfigException(message: String) : HanabiException(message)

enum class ConfigEnum constructor(val key: String, val adv: String) {

    ////////////////////// InetSocketAddressConfigHelper

    SERVER_NAME("server.name", "server.name 是本机的服务名，集群内应唯一"),

    CLIENT_ADDR("client.addr", "client.addr 的配置格式应由如下组成：client.addr.{服务名}:{选举leader使用端口号}:{集群内机器通讯使用端口号}"),

    ////////////////////// LogConfigHelper

    LOG_INDEX_INTERVAL("log.indexInterval", "log.IndexInterval 是操作日志索引生成时的字节间隔，有助于节省空间，不设定的太小都可"),

    LOG_MAX_INDEX_SIZE("log.maxIndexSize", "log.maxIndexSize 日志文件最大只能为这么大，太大了影响索引效率，日志分片受其影响，如果索引文件满了，那么将会创建新的日志分片。"),

    LOG_MAX_MESSAGE_SIZE("log.maxMessageSize", "log.maxMessageSize 是操作日志最大的大小，它也影响我们的操作 key + value 最大的大小"),

    LOG_MAX_SEGMENT_SIZE("log.maxSegmentSize", "log.maxSegmentSize 日志分片文件大小"),

    ////////////////////// ElectConfigHelper

    ELECT_ELECTION_TIMEOUT_MS("elect.electionTimeoutMs", "超过 ms 没收取到 leader 的心跳就会发起选举"),

    ELECT_VOTES_BACK_OFF_MS("elect.votesBackOffMs", "选举期间拉票频率，建议为 electionTimeoutMs 的一半"),

    ELECT_HEART_BEAT_MS("elect.heartBeatMs", "心跳包，建议为 electionTimeoutMs 的一半"),

    ////////////////////// CoordinateConfigHelper

    COORDINATE_FETCH_BACK_OFF_MS("coordinate.fetchBackOffMs", "间隔 ms 去 leader 拉取最新的日志")
}
