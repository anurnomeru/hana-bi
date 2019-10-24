package com.anur.engine.storage.memory

import com.anur.engine.storage.core.VAHEKVPair
import com.anur.engine.storage.core.VerAndHanabiEntry
import java.util.PriorityQueue
import java.util.TreeMap

/**
 * Created by Anur IjuoKaruKas on 2019/10/11
 *
 * 内存存储实现，支持 mvcc （提交部分）
 */
object MemoryMVCCStorageCommittedPart {

    private val dataKeeper = TreeMap<String, VerAndHanabiEntry>()

    private val holdKeysMapping = mutableMapOf<Long, List<VAHEKVPair>>()

    fun commonOperate(trxId: Long, pairs: List<VAHEKVPair>) {
        for (pair in pairs) {
            dataKeeper.compute(pair.key) { _, currentVersion ->
                pair.value.also { it.currentVersion = currentVersion }
            }
        }
        holdKeysMapping[trxId] = pairs
    }
}