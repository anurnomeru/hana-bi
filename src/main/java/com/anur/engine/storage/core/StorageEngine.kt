package com.anur.engine.storage.core

/**
 * Created by Anur IjuoKaruKas on 2019/9/17
 */
interface StorageEngine {

    fun commonStartTrx(): Long

    fun commonCommitTrx(trxId: Long)

    fun strSelect(key: String, trxId: Long? = null): HanabiEntry

    fun strInsert(key: String, entry: HanabiEntry)

    fun strUpdate(key: String, entry: HanabiEntry)

    fun strUpsert(key: String, entry: HanabiEntry)

    fun strDelete(key: String, trxId: Long? = null)
}