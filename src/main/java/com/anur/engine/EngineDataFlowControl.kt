package com.anur.engine

import com.anur.core.log.Debugger
import com.anur.core.log.DebuggerLevel
import com.anur.core.struct.base.Operation
import com.anur.engine.api.constant.CommandTypeConst
import com.anur.engine.api.constant.common.CommonApiConst
import com.anur.engine.api.constant.str.StrApiConst
import com.anur.engine.queryer.EngineDataQueryer
import com.anur.engine.result.common.ParameterHandler
import com.anur.engine.result.EngineResult
import com.anur.engine.result.common.EngineExecutor
import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.memory.MemoryMVCCStorageUnCommittedPart
import com.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import com.anur.engine.trx.manager.TrxManager
import com.anur.exception.RollbackException

/**
 * Created by Anur IjuoKaruKas on 2019/10/24
 *
 * 数据流转控制，具体参考文档 TrxDesign.MD
 */
object EngineDataFlowControl {

    private val logger = Debugger(EngineDataFlowControl.javaClass).switch(DebuggerLevel.INFO)

    fun commandInvoke(opera: Operation): EngineResult {
        val engineExecutor = EngineExecutor(EngineResult())
        val parameterHandler = ParameterHandler(opera)
        engineExecutor.setParameterHandler(parameterHandler)

        try {
            /*
             * common 操作比较特殊，它直接会有些特殊交互，比如开启一个事务，关闭一个事务等。
             */
            when (parameterHandler.storageType) {
                CommandTypeConst.COMMON -> {
                    when (parameterHandler.api) {
                        CommonApiConst.START_TRX -> {
                            logger.trace("事务 [${parameterHandler.trxId}] 已经开启")
                            return engineExecutor.engineResult // TODO
                        }
                        CommonApiConst.COMMIT_TRX -> {
                            doCommit(parameterHandler.trxId)
                            return engineExecutor.engineResult // TODO
                        }
                        CommonApiConst.ROLL_BACK -> {
                            throw RollbackException()
                        }
                    }
                }

                CommandTypeConst.STR -> {
                    when (parameterHandler.api) {
                        StrApiConst.SELECT -> {
                            doQuery(engineExecutor)
                        }
                        StrApiConst.DELETE -> {
                            doAcquire(engineExecutor, HanabiEntry.Companion.OperateType.DISABLE)
                        }
                        StrApiConst.SET -> {
                            doAcquire(engineExecutor, HanabiEntry.Companion.OperateType.ENABLE)
                        }
                        StrApiConst.SET_EXIST -> {
                            EngineDataQueryer.doQuery(engineExecutor)
                            engineExecutor.hanabiEntry()
                                    ?.also { engineExecutor.shotFailure() }
                                    ?: also {
                                        doAcquire(engineExecutor, HanabiEntry.Companion.OperateType.ENABLE)
                                    }
                        }
                        StrApiConst.SET_NOT_EXIST -> {
                            EngineDataQueryer.doQuery(engineExecutor)
                            engineExecutor.hanabiEntry()
                                    ?.also {
                                        doAcquire(engineExecutor, HanabiEntry.Companion.OperateType.ENABLE)
                                    }
                                    ?: also { engineExecutor.shotFailure() }
                        }
                        StrApiConst.SET_IF -> {
                            EngineDataQueryer.doQuery(engineExecutor)
                            val currentValue = engineExecutor.hanabiEntry()?.value
                            val expectValue = parameterHandler.values[1]

                            if (expectValue == currentValue) {
                                doAcquire(engineExecutor, HanabiEntry.Companion.OperateType.ENABLE)
                            } else {
                                engineExecutor.shotFailure()
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
            engineExecutor.exceptionCaught(e)
        }
        return engineExecutor.engineResult
    }

    private fun doQuery(engineExecutor: EngineExecutor) {
        EngineDataQueryer.doQuery(engineExecutor)
        logger.trace("事务 [${engineExecutor.getParameterHandler().trxId}] 对 key [${engineExecutor.getParameterHandler().key}] 进行了查询操作" +
                " 数据位于 ${engineExecutor.engineResult.queryExecutorDefinition} 值为 ==> {${engineExecutor.engineResult.getHanabiEntry()}}")
    }

    /**
     * 进行事务控制与数据流转，通过无锁控制来进行阻塞与唤醒
     *
     * 如果拿到锁，则调用api，将数据插入 未提交部分(uc)
     */
    private fun doAcquire(engineExecutor: EngineExecutor, operateType: HanabiEntry.Companion.OperateType) {
        val parameterHandler = engineExecutor.getParameterHandler()
        parameterHandler.operateType = operateType
        TrxFreeQueuedSynchronizer.acquire(parameterHandler.trxId, parameterHandler.key) {
            MemoryMVCCStorageUnCommittedPart.commonOperate(parameterHandler)
        }
        logger.trace("事务 [${engineExecutor.getParameterHandler().trxId}] 将 key [${engineExecutor.getParameterHandler().key}] 设置为了" +
                " {${engineExecutor.getParameterHandler().values[0]}}")
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
        logger.trace("事务 [${trxId}] 已经提交")
    }

    private fun doRollBack(trxId: Long) {
        // todo 还没写 懒得写
    }
}
