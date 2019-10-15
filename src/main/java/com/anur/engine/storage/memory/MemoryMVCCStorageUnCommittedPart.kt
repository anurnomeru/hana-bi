package com.anur.engine.storage.memory

import com.anur.engine.api.common.constant.StorageTypeConst
import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.core.VAHEKVPair
import com.anur.engine.storage.core.VerAndHanabiEntry
import com.anur.exception.MemoryMVCCStorageUnCommittedPartException
import java.util.PriorityQueue
import java.util.TreeMap

/**
 * Created by Anur IjuoKaruKas on 2019/10/11
 *
 * 内存存储实现，支持 mvcc （未提交部分）
 */
object MemoryMVCCStorageUnCommittedPart {

    private val treeMap = TreeMap<String, VerAndHanabiEntry>()

    fun commonOperate(trxId: Long, key: String, value: String, operateType: HanabiEntry.Companion.OperateType) {
        if (treeMap.containsKey(key)) {
            throw MemoryMVCCStorageUnCommittedPartException("mvcc uc部分出现了奇怪的bug，讲道理一个 key 只会对应一个 val，注意无锁控制是否有问题！")
        } else {
            treeMap[key] = VerAndHanabiEntry(trxId, HanabiEntry(StorageTypeConst.STR, value, operateType), null)
        }
    }

    fun flushToCommittedPart(trxId: Long, holdKeys: MutableSet<String>) {
        val pairList = holdKeys.map {
            VAHEKVPair(it,
                treeMap[it] ?: throw MemoryMVCCStorageUnCommittedPartException("mvcc uc部分出现了奇怪的bug，讲道理holdKeys拥有所有key的值，注意无锁控制是否有问题！"))
        }
    }

}
