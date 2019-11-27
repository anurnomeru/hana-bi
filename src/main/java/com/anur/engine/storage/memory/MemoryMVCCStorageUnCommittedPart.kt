package com.anur.engine.storage.memory

import com.alibaba.fastjson.JSON
import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.core.VAHEKVPair
import com.anur.engine.storage.core.VerAndHanabiEntry
import com.anur.exception.MemoryMVCCStorageUnCommittedPartException
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Created by Anur IjuoKaruKas on 2019/10/11
 *
 * 内存存储实现，支持 mvcc （未提交部分）
 */
object MemoryMVCCStorageUnCommittedPart {

    private val logger = LoggerFactory.getLogger(MemoryMVCCStorageUnCommittedPart::class.java)
    private val treeMap = TreeMap<String, VerAndHanabiEntry>()

    /**
     * 查找未提交的 hanabiEntry，传入的 trxId 必须为此 key 持有的那个事务id才可以查到，否则
     * 由于隔离性，未提交的不可以查出来
     */
    fun queryKeyInTrx(trxId: Long, key: String): HanabiEntry? {
        val verAndHanabiEntry = treeMap[key]
        if (verAndHanabiEntry != null && verAndHanabiEntry.trxId == trxId) {
            return verAndHanabiEntry.hanabiEntry
        }
        return null
    }

    fun commonOperate(trxId: Long, key: String, hanabiEntry: HanabiEntry) {
        if (treeMap.containsKey(key) && treeMap[key]!!.trxId != trxId) {
            throw MemoryMVCCStorageUnCommittedPartException("mvcc uc部分出现了奇怪的bug，讲道理一个 key 只会对应一个 val，注意无锁控制 TrxFreeQueuedSynchronizer 是否有问题！")
        } else {
            logger.debug("key [$key] val [${hanabiEntry.value}] 已经进入待提交区域")
            treeMap[key] = VerAndHanabiEntry(trxId, hanabiEntry)
        }
    }

    fun flushToCommittedPart(trxId: Long, holdKeys: MutableSet<String>) {
        val vAHEKVPairs = holdKeys.map {
            VAHEKVPair(it,
                    treeMap.remove(it)
                            ?: throw MemoryMVCCStorageUnCommittedPartException("mvcc uc部分出现了奇怪的bug，讲道理holdKeys拥有所有key的值，注意无锁控制是否有问题！"))
        }
        MemoryMVCCStorageCommittedPart.commonOperate(trxId, vAHEKVPairs)
    }
}
