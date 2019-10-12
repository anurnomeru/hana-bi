package com.anur.engine.api.common.constant.str

import com.anur.exception.UnSupportedApiException


/**
 * Created by Anur IjuoKaruKas on 2019/9/18
 */
enum class StrApiConst(val byte: Byte) {

    SELECT(-128),

    INSERT(-127),

    DELETE(-126),

    UPDATE(-125),
    ;

    companion object {
        private val MAPPER = HashMap<Byte, StrApiConst>()

        init {
            for (value in StrApiConst.values()) {
                MAPPER[value.byte] = value
            }
        }

        fun map(byte: Byte): StrApiConst {
            return MAPPER[byte] ?: throw UnSupportedApiException()
        }
    }
}