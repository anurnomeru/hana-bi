package com.anur.engine.storage.memory

import com.anur.engine.storage.core.HanabiEntry
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2019/10/23
 */
object MemoryLSM {

    private val dataKeeper = HashMap<String, HanabiEntry>()

    fun get(key: String): HanabiEntry? {
        return dataKeeper[key]
    }

    fun put(key: String, entry: HanabiEntry) {
        dataKeeper[key] = entry
    }
}
