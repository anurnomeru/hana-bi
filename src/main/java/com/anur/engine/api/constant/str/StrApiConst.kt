package com.anur.engine.api.constant.str


/**
 * Created by Anur IjuoKaruKas on 2019/9/18
 */
object StrApiConst {

    const val SELECT: Byte = -128

    /**
     * 不管存不存在都删除（效率高）
     */
    const val DELETE: Byte = -125

    /**
     * 不管存不存在都插入（效率高）
     */
    const val SET: Byte = -127

    /**
     * 存在才会写入
     */
    const val SET_EXIST: Byte = -126

    /**
     * 不存在才会写入
     */
    const val SET_NOT_EXIST: Byte = -125
}