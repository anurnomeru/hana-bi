package com.anur.engine.memory

import com.anur.engine.common.entry.ByteBufferHanabiEntry
import java.util.*

/**
 * Created by Anur IjuoKaruKas on 2019/12/4
 */
class MemoryLSMChain {

    /**
     * 存储空间评估
     */
    var memoryAssess: Int = 0

    var nextChain: MemoryLSMChain? = null

    val dataKeeper = TreeMap<String, ByteBufferHanabiEntry>()

    fun get(key: String): ByteBufferHanabiEntry? {
        return dataKeeper[key] ?: nextChain?.get(key)
    }
}