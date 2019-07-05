package ink.anur.config

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
class ConfigHelper {

    @Volatile
    internal var RESOURCE_BUNDLE: ResourceBundle

    private var READ_LOCK: Lock
    private var WRITE_LOCK: Lock
    private val CACHE = ConcurrentHashMap<ConfigEnum, Any>()
    private val ERROR_FORMATTER = "读取application.properties配置异常，异常项目：%s，建议：%s"

    init {
        val readWriteLock = ReentrantReadWriteLock()
        READ_LOCK = readWriteLock.readLock()
        WRITE_LOCK = readWriteLock.writeLock()
        RESOURCE_BUNDLE = ResourceBundle.getBundle("application")
    }

    /**
     * 优先获取缓存中的值，如果获取不到再从配置文件获取
     */
    private fun <T> lockSupplier(configEnum: ConfigEnum, supplier: () -> T): T {
        var t: T
        try {
            READ_LOCK.lock()
            CACHE.containsKey(configEnum)

            t = (if (CACHE.containsKey(configEnum)) {
                CACHE[configEnum]
            } else {
                t = supplier.invoke()
                CACHE[configEnum] = t!!
            }) as T

        } finally {
            READ_LOCK.unlock()
        }
        return t
    }

    /**
     * 刷新配置
     */
    fun refresh() {
        try {
            WRITE_LOCK.lock()
            CACHE.clear()
            RESOURCE_BUNDLE = ResourceBundle.getBundle("application")
        } finally {
            WRITE_LOCK.unlock()
        }
    }

    /**
     * 根据key获取某个配置
     */
    protected fun <T> getConfig(configEnum: ConfigEnum, transfer: (String) -> T): T {
        return lockSupplier(configEnum) {
            transfer.invoke(RESOURCE_BUNDLE.getString(configEnum.key))
                ?: throw ApplicationConfigException(String.format(ERROR_FORMATTER, configEnum.key, configEnum.adv))
        }
    }

    /**
     * 根据key模糊得获取某些配置，匹配规则为 key%
     */
    protected fun <T> getConfigSimilar(configEnum: ConfigEnum, transfer: (Pair<String, String>) -> T): List<T> {
        return lockSupplier(configEnum) {
            val stringEnumeration = RESOURCE_BUNDLE.keys
            val keys = mutableListOf<String>()

            while (stringEnumeration.hasMoreElements()) {
                val k = stringEnumeration.nextElement()
                if (k.startsWith(configEnum.key)) {
                    keys.add(k)
                }
            }

            val key = configEnum.key
            keys.map {
                transfer.invoke(
                    Pair(
                        if (it.length > key.length) it.substring(key.length + 1) else it,
                        RESOURCE_BUNDLE.getString(it)
                    ))
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

fun main() {
}