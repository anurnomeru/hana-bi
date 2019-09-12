package com.anur.config

import com.anur.config.common.ConfigHelper
import com.anur.config.common.ConfigurationEnum

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 *
 * 日志配置相关读取类
 */
class LogConfigHelper {

    companion object : ConfigHelper() {
        private var relativelyPath: String = System.getProperty("user.dir")

        fun getBaseDir(): String {
            return relativelyPath + "/" + InetSocketAddressConfigHelper.getServerName()
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