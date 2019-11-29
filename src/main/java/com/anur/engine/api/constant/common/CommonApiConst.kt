package com.anur.engine.api.constant.common


/**
 * Created by Anur IjuoKaruKas on 2019/9/18
 */
object CommonApiConst {
    const val START_TRX: Byte = -128
    const val COMMIT_TRX: Byte = -127
    /**
     * 客户端主动回滚
     */
    const val ROLL_BACK: Byte = -126

    /**
     * 由于引擎以外的原因报错了，回滚
     */
    const val FORCE_ROLL_BACK: Byte = -125
}