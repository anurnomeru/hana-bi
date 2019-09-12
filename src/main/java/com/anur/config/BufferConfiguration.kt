package com.anur.config

import com.anur.config.common.ConfigHelper
import com.anur.config.common.ConfigurationEnum

object BufferConfiguration : ConfigHelper() {

    fun getMaxBufferPoolSize(): Long {
        return CoordinateConfiguration.getConfig(ConfigurationEnum.COORDINATE_FETCH_BACK_OFF_MS) { it.toLong() } as Long
    }
}