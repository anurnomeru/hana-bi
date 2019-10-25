package com.anur.engine.api.constant

import com.anur.exception.UnSupportStorageTypeException

/**
 * Created by Anur IjuoKaruKas on 2019/9/18
 */
enum class StorageTypeConst(val byte: Byte) {

    /** 这个类型表示为，此操作为基础操作 */
    COMMON(-128),

    /** 这个类型表示为，此操作为字符串操作 */
    STR(-127);

    companion object {
        private val MAPPER = HashMap<Byte, StorageTypeConst>()

        init {
            for (value in values()) {
                MAPPER[value.byte] = value
            }
        }

        fun map(byte: Byte): StorageTypeConst {
            return MAPPER[byte] ?: throw UnSupportStorageTypeException()
        }
    }
}