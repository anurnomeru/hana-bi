package ink.anur.config

import ink.anur.config.common.ConfigEnum
import ink.anur.config.common.ConfigHelper

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 */
object CoordinateConfiguration : ConfigHelper() {

    fun getFetchBackOfMs(): Int {
        return getConfig(ConfigEnum.COORDINATE_FETCH_BACK_OFF_MS) { Integer.valueOf(it) } as Int
    }
}