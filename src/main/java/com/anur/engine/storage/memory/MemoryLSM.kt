package com.anur.engine.storage.memory

import com.alibaba.fastjson.JSON
import com.anur.engine.api.constant.StorageTypeConst
import com.anur.engine.storage.core.HanabiEntry
import org.slf4j.LoggerFactory
import kotlin.collections.HashMap

/**
 * Created by Anur IjuoKaruKas on 2019/10/23
 */
object MemoryLSM {

    private val logger = LoggerFactory.getLogger(MemoryLSM::class.java)
    private val dataKeeper = HashMap<String, HanabiEntry>()

    fun put(key: String, entry: HanabiEntry) {
        dataKeeper[key] = entry
    }

    fun print() {
        logger.debug(JSON.toJSONString(dataKeeper))
    }
}

fun main() {
    MemoryLSM.put("zzz", HanabiEntry(StorageTypeConst.STR, "zzzz", HanabiEntry.Companion.OperateType.ENABLE))
    MemoryLSM.put("zzz1", HanabiEntry(StorageTypeConst.STR, "zzzz", HanabiEntry.Companion.OperateType.ENABLE))
    MemoryLSM.put("zzz", HanabiEntry(StorageTypeConst.STR, "zzzz", HanabiEntry.Companion.OperateType.ENABLE))
    MemoryLSM.print()
}