package com.anur.io.engine.storage

import com.anur.io.engine.storage.core.HanabiEntry


/**
 * Created by Anur IjuoKaruKas on 2019/9/17
 */
interface StorageEngine {

    fun select(key: String, trxId: Long? = null): HanabiEntry

    fun insert(key: String, entry: HanabiEntry)

    fun update(key: String, entry: HanabiEntry)

    fun upsert(key: String, entry: HanabiEntry)

    fun delete(key: String, trxId: Long? = null)

    fun startTrx(): Long

    fun commit(trxId: Long)
}