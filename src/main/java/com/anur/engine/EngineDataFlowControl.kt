package com.anur.engine

import com.anur.core.log.Debugger
import com.anur.core.struct.base.Operation
import com.anur.engine.api.constant.CommandTypeConst
import com.anur.engine.api.constant.common.CommonApiConst
import com.anur.engine.api.constant.str.StrApiConst
import com.anur.engine.queryer.EngineDataQueryer
import com.anur.engine.result.common.ParameterHandler
import com.anur.engine.result.EngineResult
import com.anur.engine.result.common.ResultHandler
import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.memory.MemoryMVCCStorageUnCommittedPartExecutor
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

    private val logger = Debugger(EngineDataFlowControl.javaClass)

    fun commandInvoke(opera: Operation): EngineResult {
        val resultHandler = ResultHandler(EngineResult())
        val parameterHandler = ParameterHandler(opera)
        resultHandler.setParameterHandler(parameterHandler)

        try {
            /*
             * common 操作比较特殊，它直接会有些特殊交互，比如开启一个事务，关闭一个事务等。
             */
            when (parameterHandler.storageType) {
                CommandTypeConst.COMMON -> {
                    when (parameterHandler.api) {
                        CommonApiConst.START_TRX -> {
                            logger.trace("事务 [${parameterHandler.trxId}] 已经开启")
                            return resultHandler.engineResult // TODO
                        }
                        CommonApiConst.COMMIT_TRX -> {
                            doCommit(parameterHandler.trxId)
                            logger.trace("事务 [${parameterHandler.trxId}] 已经提交（数据会冲刷到 MVCC CommitPart）")
                            return resultHandler.engineResult // TODO
                        }
                        CommonApiConst.ROLL_BACK -> {
                            throw RollbackException()
                        }
                    }
                }

                CommandTypeConst.STR -> {
                    when (parameterHandler.api) {
                        StrApiConst.SELECT -> {
                            EngineDataQueryer.doQuery(resultHandler)
                        }
                        StrApiConst.DELETE -> {
                            doAcquire(resultHandler, HanabiEntry.Companion.OperateType.DISABLE)
                        }
                        StrApiConst.SET -> {
                            doAcquire(resultHandler, HanabiEntry.Companion.OperateType.ENABLE)
                        }
                        StrApiConst.SET_EXIST -> {
                            EngineDataQueryer.doQuery(resultHandler)
                            resultHandler.hanabiEntry()
                                    ?.also { resultHandler.shotFailure() }
                                    ?: also {
                                        doAcquire(resultHandler, HanabiEntry.Companion.OperateType.ENABLE)
                                    }
                        }
                        StrApiConst.SET_NOT_EXIST -> {
                            EngineDataQueryer.doQuery(resultHandler)
                            resultHandler.hanabiEntry()
                                    ?.also {
                                        doAcquire(resultHandler, HanabiEntry.Companion.OperateType.ENABLE)
                                    }
                                    ?: also { resultHandler.shotFailure() }
                        }
                        StrApiConst.SET_IF -> {
                            EngineDataQueryer.doQuery(resultHandler)
                            val currentValue = resultHandler.hanabiEntry()?.value
                            val expectValue = parameterHandler.values[1]

                            if (expectValue == currentValue) {
                                doAcquire(resultHandler, HanabiEntry.Companion.OperateType.ENABLE)
                            } else {
                                resultHandler.shotFailure()
                            }
                        }
                    }
                }
            }

            if (parameterHandler.shortTransaction) doCommit(parameterHandler.trxId)
        } catch (e: Throwable) {
            logger.error("存储引擎执行出错，将执行回滚，原因 [$e.message]")
            e.printStackTrace()

            doRollBack(parameterHandler.trxId)
            resultHandler.exceptionCaught(e)
        }
        return resultHandler.engineResult
    }

    /**
     * 进行事务控制与数据流转，通过无锁控制来进行阻塞与唤醒
     *
     * 如果拿到锁，则调用api，将数据插入 未提交部分(uc)
     */
    private fun doAcquire(resultHandler: ResultHandler, operateType: HanabiEntry.Companion.OperateType) {
        val parameterHandler = resultHandler.getParameterHandler()
        parameterHandler.operateType = operateType
        TrxFreeQueuedSynchronizer.acquire(parameterHandler.trxId, parameterHandler.key) {
            MemoryMVCCStorageUnCommittedPartExecutor.commonOperate(parameterHandler)
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
            keys?.let { MemoryMVCCStorageUnCommittedPartExecutor.flushToCommittedPart(trxId, it) }
            TrxManager.releaseTrx(trxId)
        }
    }

    private fun doRollBack(trxId: Long) {
        // todo 还没写 懒得写
    }
}
