package com.anur.engine.api.constant.str


/**
 * Created by Anur IjuoKaruKas on 2019/9/18
 */
object StrApiConst {

    const val SELECT: Byte = -128

    /**
     * 不管存不存在都删除（效率高）
     */
    const val DELETE: Byte = -127

    /**
     * 不管存不存在都插入（效率高）
     */
    const val SET: Byte = -126

    /**
     * 存在才会写入
     */
    const val SET_EXIST: Byte = -125

    /**
     * 不存在才会写入
     */
    const val SET_NOT_EXIST: Byte = -124

    /**
     * 如果值为 xx 才写入
     */
    const val SET_IF: Byte = -123
}