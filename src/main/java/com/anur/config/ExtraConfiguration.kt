package com.anur.config

import ink.anur.config.CoordinateConfiguration
import ink.anur.config.common.ConfigHelper
import ink.anur.config.common.ConfigurationEnum

/**
 * Created by Anur IjuoKaruKas on 2019/7/14
 */
class ExtraConfiguration {

    companion object : ConfigHelper() {
        fun isDebug(): Boolean {
            return CoordinateConfiguration.getConfig(ConfigurationEnum.DEBUG_MODE) { "enable" == it } as Boolean
        }
    }
}