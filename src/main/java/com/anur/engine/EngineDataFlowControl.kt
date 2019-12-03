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
import com.anur.engine.trx.watermark.WaterMarker
import com.anur.exception.RollbackException
import com.anur.exception.WaterMarkCreationException

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
        val values = cmd.getValues()
        val trxId = cmd.getTrxId()
        val storageType = StorageTypeConst.map(cmd.getType())
        var waterMarker = WaterMarkRegistry.findOut(trxId)

        /*
         * 如果是短事务，操作完就直接提交
         *
         * 如果是长事务，则如果没有激活过事务，需要进行事务的激活(创建快照)
         */
        val isShortTrx = TransactionTypeConst.map(cmd.getTransactionType()) == TransactionTypeConst.SHORT

        // 如果没有水位快照，代表此事务从未激活过，需要去激活一下
        if (waterMarker == WaterMarker.NONE) {
            if (isShortTrx) {
                TrxManager.activateTrx(trxId, false)
            } else {
                TrxManager.activateTrx(trxId, true)

                // 从事务控制器申请激活该事务
                waterMarker = WaterMarkRegistry.findOut(trxId)

                if (waterMarker == WaterMarker.NONE) {
                    throw WaterMarkCreationException("事务 [$trxId] 创建事务快照失败")
                }
            }
        }

        try {
            /*
             * common 操作比较特殊，它直接会有些特殊交互，比如开启一个事务，关闭一个事务等。
             */
            when (storageType) {
                StorageTypeConst.COMMON -> {
                    when (cmd.getApi()) {
                        CommonApiConst.START_TRX -> {
                            logger.trace("事务 [$trxId] 已经开启")
                            return result
                        }
                        CommonApiConst.COMMIT_TRX -> {
                            doCommit(trxId)
                            logger.trace("事务 [$trxId] 已经提交（数据会冲刷到 MVCC CommitPart）")
                            return result
                        }
                        CommonApiConst.ROLL_BACK -> {
                            throw RollbackException()
                        }
                    }
                }

                StorageTypeConst.STR -> {
                    when (cmd.getApi()) {
                        StrApiConst.SELECT -> {
                            result.hanabiEntry = EngineDataQueryer.doQuery(trxId, key, waterMarker)
                        }
                        StrApiConst.DELETE -> {
                            doAcquire(trxId, key, values[0], StorageTypeConst.STR, HanabiEntry.Companion.OperateType.DISABLE)
                        }
                        StrApiConst.SET -> {
                            doAcquire(trxId, key, values[0], StorageTypeConst.STR, HanabiEntry.Companion.OperateType.ENABLE)
                        }
                        StrApiConst.SET_EXIST -> {
                            result.hanabiEntry = EngineDataQueryer.doQuery(trxId, key, waterMarker)
                            if (result.hanabiEntry == null) {
                                doAcquire(trxId, key, values[0], StorageTypeConst.STR, HanabiEntry.Companion.OperateType.ENABLE)
                            } else {
                                result.result = false
                            }
                        }
                        StrApiConst.SET_NOT_EXIST -> {
                            result.hanabiEntry = EngineDataQueryer.doQuery(trxId, key, waterMarker)
                            if (result.hanabiEntry != null) {
                                doAcquire(trxId, key, values[0], StorageTypeConst.STR, HanabiEntry.Companion.OperateType.ENABLE)
                            } else {
                                result.result = false
                            }
                        }
                        StrApiConst.SET_IF -> {
                            result.hanabiEntry = EngineDataQueryer.doQuery(trxId, key, waterMarker)
                            if (values[1] == result.hanabiEntry?.value) {
                                doAcquire(trxId, key, values[0], StorageTypeConst.STR, HanabiEntry.Companion.OperateType.ENABLE)
                            } else {
                                result.result = false
                            }
                        }
                    }
                }
            }

            if (isShortTrx) doCommit(trxId)
        } catch (e: Throwable) {
            logger.error("存储引擎执行出错，将执行回滚，原因 [$e.message]")
            doRollBack(trxId)
            result.result = false
            result.err = e
        }

        return result
    }

    /**
     * 进行事务控制与数据流转，通过无锁控制来进行阻塞与唤醒
     *
     * 如果拿到锁，则调用api，将数据插入 未提交部分(uc)
     */
    private fun doAcquire(trxId: Long, key: String, value: String, storageType: StorageTypeConst, operateType: HanabiEntry.Companion.OperateType) {
        TrxFreeQueuedSynchronizer.acquire(trxId, key) {
            MemoryMVCCStorageUnCommittedPart.commonOperate(trxId, key, HanabiEntry(storageType, value, operateType)
            )
        }
    }

    /**
     * 进行事务控制与数据流转，
     * 1、首先要从无锁控制释放该锁，我们的很多操作都是经由 TrxFreeQueuedSynchronizer 进行控制锁并发的
     * 2、将数据推入 commitPart
     * 3、通知事务控制器，事务已经被销毁
     */
    private fun doCommit(trxId: Long) {
        TrxFreeQueuedSynchronizer.release(trxId) { keys ->
            keys?.let { MemoryMVCCStorageUnCommittedPart.flushToCommittedPart(trxId, it) }
            TrxManager.releaseTrx(trxId)
        }
    }

    private fun doRollBack(trxId: Long) {
        // todo 还没写 懒得写
    }
}
