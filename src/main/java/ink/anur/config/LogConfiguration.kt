package ink.anur.config

import com.anur.config.InetSocketAddressConfigHelper
import ink.anur.config.common.ConfigEnum
import ink.anur.config.common.ConfigHelper

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 *
 * 日志配置相关读取类
 */
object LogConfiguration : ConfigHelper() {
    private var relativelyPath: String = System.getProperty("user.dir")

    fun getBaseDir(): String {
        return relativelyPath + "\\" + InetSocketAddressConfigHelper.getServerName()
    }

    fun getIndexInterval(): Int {
        return getConfig(ConfigEnum.LOG_INDEX_INTERVAL) { Integer.valueOf(it) } as Int
    }

    fun getMaxIndexSize(): Int {
        return getConfig(ConfigEnum.LOG_MAX_INDEX_SIZE) { Integer.valueOf(it) } as Int
    }

    fun getMaxLogMessageSize(): Int {
        return getConfig(ConfigEnum.LOG_MAX_MESSAGE_SIZE) { Integer.valueOf(it) } as Int
    }

    fun getMaxLogSegmentSize(): Int {
        return getConfig(ConfigEnum.LOG_MAX_SEGMENT_SIZE) { Integer.valueOf(it) } as Int
    }
}