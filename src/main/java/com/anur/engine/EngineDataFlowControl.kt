package com.anur.engine

import com.anur.core.log.Debugger
import com.anur.core.log.DebuggerLevel
import com.anur.core.struct.base.Operation
import com.anur.engine.api.constant.StorageTypeConst
import com.anur.engine.api.constant.TransactionTypeConst
import com.anur.engine.api.constant.common.CommonApiConst
import com.anur.engine.api.constant.str.StrApiConst
import com.anur.engine.queryer.EngineDataQueryer
import com.anur.engine.result.EngineResult
import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.memory.MemoryMVCCStorageUnCommittedPart
import com.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import com.anur.engine.trx.manager.TrxManager
import com.anur.engine.trx.watermark.WaterMarkRegistry
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2019/10/24
 *
 * 数据流转控制，具体参考文档 TrxDesign.MD
 */
object EngineDataFlowControl {

    private val logger = Debugger(EngineDataFlowControl.javaClass).switch(DebuggerLevel.INFO)

    fun commandInvoke(opera: Operation): EngineResult {
        val result = EngineResult()

        val cmd = opera.hanabiCommand
        val key = opera.key
        val value = cmd.getValue()
        val trxId = cmd.getTrxId()
        val storageType = StorageTypeConst.map(cmd.getType())

        val waterMarker = WaterMarkRegistry.findOut(trxId)

        /*
         * common 操作比较特殊，它直接会有些特殊交互，比如开启一个事务，关闭一个事务等。
         */
        when (storageType) {
            StorageTypeConst.COMMON -> {
                when (cmd.getApi()) {
                    CommonApiConst.START_TRX -> {
                        logger.trace("事务 [$trxId] 已经开启（实际上没有什么卵用，在申请的时候就开启了）")
                        return result
                    }
                    CommonApiConst.COMMIT_TRX -> {
                        doCommit(trxId)
                        logger.trace("事务 [$trxId] 已经提交（数据会冲刷到 MVCC CommitPart）")
                        return result
                    }
                }
            }
        }

        when (storageType) {
            StorageTypeConst.STR -> {
                when (cmd.getApi()) {
                    StrApiConst.SELECT -> {
                        result.hanabiEntry = EngineDataQueryer.doQuery(trxId, key, waterMarker)
                        logger.trace("事务 [$trxId] 查询 key [$key] \n" +
                                "      >> ${result.hanabiEntry}")
                    }
                    StrApiConst.INSERT -> {
                        doAcquire(trxId, key, value, StorageTypeConst.STR, HanabiEntry.Companion.OperateType.ENABLE)
                        logger.trace("事务 [$trxId] 将插入数据 key [$key] val [$value]")
                    }
                    StrApiConst.UPDATE -> {
                        doAcquire(trxId, key, value, StorageTypeConst.STR, HanabiEntry.Companion.OperateType.ENABLE)
                        logger.trace("事务 [$trxId] 将修改数据 key [$key] --> val [$value]")
                    }
                    StrApiConst.DELETE -> {
                        doAcquire(trxId, key, value, StorageTypeConst.STR, HanabiEntry.Companion.OperateType.DISABLE)
                        logger.trace("事务 [$trxId] 将删除数据 key [$key]")
                    }

                }
            }
        }

        /*
         * 如果是短事务，操作完就直接提交
         */
        val isShortTrx = TransactionTypeConst.map(cmd.getTransactionType()) == TransactionTypeConst.SHORT
        if (isShortTrx) doCommit(trxId)
        return result
    }

    /**
     * 进行事务控制与数据流转，通过无锁控制来进行阻塞与唤醒
     *
     * 如果拿到锁，则调用api，将数据插入 未提交部分(uc)
     */
    private fun doAcquire(trxId: Long, key: String, value: String, storageType: StorageTypeConst, operateType: HanabiEntry.Companion.OperateType) {
        TrxFreeQueuedSynchronizer.acquire(trxId, key) {
            MemoryMVCCStorageUnCommittedPart.commonOperate(trxId, key,
                    HanabiEntry(storageType, value, operateType)
            )
        }
    }

    /**
     * 进行事务控制与数据流转
     */
    private fun doCommit(trxId: Long) {
        TrxFreeQueuedSynchronizer.release(trxId) { keys ->
            keys?.let { MemoryMVCCStorageUnCommittedPart.flushToCommittedPart(trxId, it) }
            TrxManager.releaseTrx(trxId)
        }
    }
}
