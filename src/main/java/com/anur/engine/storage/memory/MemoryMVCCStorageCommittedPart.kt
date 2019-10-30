package com.anur.engine.storage.memory

import com.anur.core.lock.rentrant.ReentrantLocker
import com.anur.engine.api.constant.StorageTypeConst
import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.core.VAHEKVPair
import com.anur.engine.storage.core.VerAndHanabiEntry
import com.anur.engine.trx.manager.TrxManager
import com.anur.util.HanabiExecutors
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
        HanabiExecutors.execute(
                Runnable {
                    while (true) {
                        val takeNotify = TrxManager.takeNotify()
                        val headMap = holdKeysMapping.headMap(takeNotify, true)

                        // TODO 虽然 pollLast 效率不是太高，但是明显从大到小去移除事务会快一些
                        // TODO 先这么写着试试
                        val pollLastEntry = headMap.pollLastEntry()

                        while (pollLastEntry != null) {
                            val trxId = pollLastEntry.key

                            // 释放单个key
                            for (pair in pollLastEntry.value) {
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

                            // 释放整个trxId
                            holdKeysMapping.remove(trxId)
                        }
                    }
                })
    }

    /**
     * 递归提交某个VAHE，及其更早的版本
     */
    private fun commitVAHERecursive(prev: VerAndHanabiEntry, key: String, removeEntry: VerAndHanabiEntry) {
        val currentVer = prev.currentVersion
        when {
            currentVer == null -> // 如果找到头都找不到，代表早就被释放掉了
                return
            currentVer.trxId < removeEntry.trxId -> // 找到更小的也没必要继续找下去了
                return
            currentVer.trxId == removeEntry.trxId -> {// 只需要提交最新的key即可
                MemoryLSM.put(key, currentVer.hanabiEntry)
                prev.currentVersion == null
            }
            else -> commitVAHERecursive(currentVer, key, removeEntry)
        }
    }
}