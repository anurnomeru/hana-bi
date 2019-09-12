package com.anur.config

import com.anur.config.common.ConfigHelper
import com.anur.config.common.ConfigurationEnum

object BufferConfiguration : ConfigHelper() {

    fun getMaxBufferPoolSize(): Long {
        return CoordinateConfiguration.getConfig(ConfigurationEnum.BUFFER_MAX_SIZE) { it.toLong() } as Long
    }
}