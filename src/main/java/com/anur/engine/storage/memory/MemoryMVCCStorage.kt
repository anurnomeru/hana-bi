package com.anur.engine.storage.memory

import com.anur.engine.api.common.constant.StorageTypeConst
import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.core.VerAndHanabiEntry
import java.util.TreeMap

/**
 * Created by Anur IjuoKaruKas on 2019/10/11
 *
 * 内存存储实现，支持 mvcc
 */
object MemoryMVCCStorage {
    val treeMap = TreeMap<String, VerAndHanabiEntry>()

    fun strInsert(trxId: Long, key: String, value: String) {
        treeMap.compute(key) { _, vahCurrent ->
            VerAndHanabiEntry(trxId, HanabiEntry(StorageTypeConst.STR, value, HanabiEntry.Companion.OperateType.INSERT), vahCurrent)
        }
    }


}