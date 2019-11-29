package com.anur.engine.storage.memory

import com.anur.core.lock.rentrant.ReentrantLocker
import com.anur.core.log.Debugger
import com.anur.core.log.DebuggerLevel
import com.anur.engine.api.constant.StorageTypeConst
import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.core.VerAndHanabiEntryWithKeyPair
import com.anur.engine.storage.core.VerAndHanabiEntry
import com.anur.engine.trx.manager.TrxManager
import com.anur.engine.trx.watermark.WaterMarker
import com.anur.util.HanabiExecutors
import java.util.concurrent.ConcurrentSkipListMap

/**
 * Created by Anur IjuoKaruKas on 2019/10/11
 *
 * 内存存储实现，支持 mvcc （提交部分）
 */
object MemoryMVCCStorageCommittedPart {

    /**
     * 数据存储使用一个map，数据键 <-> VerAndHanabiEntry
     */
    private val dataKeeper = HashMap<String, VerAndHanabiEntry>()

    /**
     * 保存一个事务持有多少 key，且从小打大排列
     */
    private val holdKeysMapping = ConcurrentSkipListMap<Long, List<VerAndHanabiEntryWithKeyPair>>()
    private val logger = Debugger(MemoryMVCCStorageCommittedPart.javaClass)
//            .switch(DebuggerLevel.INFO)
    private val locker = ReentrantLocker()

    /**
     * 和 uc 部分的有点像，但是这里要递归查找，
     * 而且，基于隔离性，且要实现可重复读
     */
    fun queryKeyInTrx(trxId: Long, key: String, waterMarker: WaterMarker): HanabiEntry? {
        var verAndHanabiEntry = dataKeeper[key]
        val waterHolder = waterMarker.waterHolder

        while (verAndHanabiEntry != null) {

            // NONE 代表不需要创建快照，而且这种短事务查询，到了这一层肯定是可见的，只要查到就可以返回
            if (waterMarker == WaterMarker.NONE ||

                    // 如果是长事务，则要求事务小于当前事务，且在创建时已经提交，才是可见的
                    verAndHanabiEntry.trxId <= trxId && !waterHolder.isActivateTrx(verAndHanabiEntry.trxId)) {
                return verAndHanabiEntry.hanabiEntry
            }
            verAndHanabiEntry = verAndHanabiEntry.currentVersion
        }
        return null
    }

    /**
     * 将 uc 部分的数据提交到 mvcc 临界控制区，这部分需要做好隔离性控制
     */
    fun commonOperate(trxId: Long, pairs: List<VerAndHanabiEntryWithKeyPair>) {
        logger.debug("事务 $trxId 已经进入 MVCC 临界控制区")
        for (pair in pairs) {
            locker.lockSupplier {
                logger.debug("key [${pair.key}] val [${pair.value.hanabiEntry.value}] 已提交到 MVCC 临界控制区")

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
        /**
         *
         */
        HanabiExecutors.execute(
                Runnable {
                    logger.info("MVCC 临界控制区已经启动，等待从水位控制 TrxManager 获取最新提交水位，并将数据提交到 LSM")
                    while (true) {
                        val takeNotify = TrxManager.takeNotify()
                        // 拿到小于等于当前最低水位的部分
                        val headMap = holdKeysMapping.headMap(takeNotify, true)

                        // 取部分中最大的事务
                        // TODO 虽然 pollLast 效率不是太高，但是由于从大到小去移除事务会快一些，先这么写着试试
                        var pollLastEntry = headMap.pollLastEntry()

                        // 迭代拿到事务
                        while (pollLastEntry != null) {
                            val trxId = pollLastEntry.key

                            // 释放单个key
                            for (pair in pollLastEntry.value) {
                                // 拿到当前键最新的一个版本
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
                            pollLastEntry = headMap.pollLastEntry()
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
                logger.debug("由事务 [${currentVer.trxId}] 提交的 key [$key] val [${currentVer.hanabiEntry.value}] 正式提交到 LSM 树，此 key 上早于 ${currentVer.trxId} 的事务将失效")
                prev.currentVersion = null// 抹除当前版本
                currentVer.currentVersion = null// 将小于此版本的抹除
            }
            else -> commitVAHERecursive(currentVer, key, removeEntry)
        }
    }

}