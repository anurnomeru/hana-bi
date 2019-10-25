package com.anur.engine.api.constant

import com.anur.exception.UnSupportTransException

/**
 * Created by Anur IjuoKaruKas on 2019/9/18
 */
enum class TransactionTypeConst(val byte: Byte) {

    /** 这个类型表示为，此操作短事务*/
    SHORT(-128),

    /** 这个类型表示为，此操作长事务*/
    LONG(-127);

    companion object {
        private val MAPPER = HashMap<Byte, TransactionTypeConst>()

        init {
            for (value in TransactionTypeConst.values()) {
                MAPPER[value.byte] = value
            }
        }

        fun map(byte: Byte): TransactionTypeConst {
            return MAPPER[byte] ?: throw UnSupportTransException()
        }
    }
}