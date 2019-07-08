package ink.anur.config

import ink.anur.config.common.ConfigHelper
import ink.anur.config.common.ConfigurationEnum

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 *
 * 日志配置相关读取类
 */
class LogConfiguration {

    companion object : ConfigHelper() {
        private var relativelyPath: String = System.getProperty("user.dir")

        fun getBaseDir(): String {
            return relativelyPath + "\\" + InetSocketAddressConfiguration.getServerName()
        }

        fun getIndexInterval(): Int {
            return getConfig(ConfigurationEnum.LOG_INDEX_INTERVAL) { Integer.valueOf(it) } as Int
        }

        fun getMaxIndexSize(): Int {
            return getConfig(ConfigurationEnum.LOG_MAX_INDEX_SIZE) { Integer.valueOf(it) } as Int
        }

        fun getMaxLogMessageSize(): Int {
            return getConfig(ConfigurationEnum.LOG_MAX_MESSAGE_SIZE) { Integer.valueOf(it) } as Int
        }

        fun getMaxLogSegmentSize(): Int {
            return getConfig(ConfigurationEnum.LOG_MAX_SEGMENT_SIZE) { Integer.valueOf(it) } as Int
        }
    }
}