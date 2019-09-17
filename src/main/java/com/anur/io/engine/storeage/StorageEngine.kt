package com.anur.io.engine.storeage

import com.anur.core.struct.base.Operation


/**
 * Created by Anur IjuoKaruKas on 2019/9/17
 */
interface StorageEngine {

    fun select(key: String, trxId: Long? = null)

    fun insert(key: String, operation: Operation, trxId: Long? = null)

    fun update(key: String, operation: Operation, trxId: Long? = null)

    fun upsert(key: String, operation: Operation, trxId: Long? = null)

    fun delete(key: String, trxId: Long? = null)

    fun startTrx(): Long

    fun commit(trxId: Long)
}