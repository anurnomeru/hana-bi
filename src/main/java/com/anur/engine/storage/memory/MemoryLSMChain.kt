package com.anur.engine.storage.memory

import com.anur.engine.storage.entry.ByteBufferHanabiEntry

/**
 * Created by Anur IjuoKaruKas on 2019/12/4
 */
class MemoryLSMChain {

    /**
     * 存储空间评估
     */
    var memoryAssess: Int = 0

    var nextChain: MemoryLSMChain? = null

    val dataKeeper = HashMap<String, ByteBufferHanabiEntry>()

    fun get(key: String): ByteBufferHanabiEntry? {
        return dataKeeper[key] ?: nextChain?.get(key)
    }
}