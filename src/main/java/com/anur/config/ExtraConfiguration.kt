package com.anur.config

import com.anur.config.common.ConfigHelper
import com.anur.config.common.ConfigurationEnum

/**
 * Created by Anur IjuoKaruKas on 2019/7/14
 */
class ExtraConfiguration {

    companion object : ConfigHelper() {
        fun isDebug(): Boolean {
            return CoordinateConfigHelper.getConfig(ConfigurationEnum.DEBUG_MODE) { "enable" == it } as Boolean
        }

        fun neverReElectAfterHasLeader(): Boolean {
            return CoordinateConfigHelper.getConfig(ConfigurationEnum.REELECT) { "true" == it } as Boolean
        }
    }
}