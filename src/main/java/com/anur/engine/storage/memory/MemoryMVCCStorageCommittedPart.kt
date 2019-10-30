package com.anur.engine.storage.memory

import com.anur.core.lock.rentrant.ReentrantLocker
import com.anur.engine.api.constant.StorageTypeConst
import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.core.VAHEKVPair
import com.anur.engine.storage.core.VerAndHanabiEntry
import com.anur.engine.trx.manager.TrxManager
import com.anur.exception.MvccException
import java.util.TreeMap
import java.util.concurrent.ConcurrentSkipListMap

/**
 * Created by Anur IjuoKaruKas on 2019/10/11
 *
 * 内存存储实现，支持 mvcc （提交部分）
 */
object MemoryMVCCStorageCommittedPart {

    private val dataKeeper = HashMap<String, VerAndHanabiEntry>()

    private val holdKeysMapping = ConcurrentSkipListMap<Long, List<VAHEKVPair>>()

    private val locker = ReentrantLocker()

    fun commonOperate(trxId: Long, pairs: List<VAHEKVPair>) {
        for (pair in pairs) {
            locker.lockSupplier {
                dataKeeper.compute(pair.key) { _, currentVersion ->
                    pair.value.also { it.currentVersion = currentVersion }
                }
            }
        }
        holdKeysMapping[trxId] = pairs
    }

    /**
     * 单链表哨兵，它没有别的作用，就是方便写代码用的
     */
    private val SENTINEL = VerAndHanabiEntry(0, HanabiEntry(StorageTypeConst.COMMON, "", HanabiEntry.Companion.OperateType.ENABLE))

    init {
        Runnable {
            val takeNotify = TrxManager.takeNotify()
            val headMap = holdKeysMapping.headMap(takeNotify, true)
            for (mutableEntry in headMap) {
                for (pair in mutableEntry.value) {
                    // 拿到当前键最新的一个版本，再进行移除
                    val head = dataKeeper[pair.key]
                    SENTINEL.currentVersion = head

                    // 提交到lsm，并且抹去mvccUndoLog
                    commitVAHERecursive(SENTINEL, pair.key, pair.value)

                    // 这种情况代表当前key已经没有任何 mvcc 日志了
                    if (SENTINEL.currentVersion == null) {
                        locker.lockSupplier {
                            // 双重锁
                            if (SENTINEL.currentVersion == null) dataKeeper.remove(pair.key)
                        }
                    }
                }
            }
        }
    }

    /**
     * 递归提交某个VAHE，及其更早的版本
     */
    private fun commitVAHERecursive(prev: VerAndHanabiEntry, key: String, removeEntry: VerAndHanabiEntry) {
        val currentVer = prev.currentVersion ?: throw MvccException("数据丢失？？？！！！")
        if (currentVer == removeEntry) {
            MemoryLSM.put(key, currentVer.hanabiEntry)// 只需要提交最新的key即可
            prev.currentVersion == null
        } else {
            commitVAHERecursive(currentVer, key, removeEntry)
        }
    }
}