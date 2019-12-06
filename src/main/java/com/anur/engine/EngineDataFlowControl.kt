package com.anur.engine

import com.anur.core.log.Debugger
import com.anur.core.struct.base.Operation
import com.anur.engine.api.constant.CommandTypeConst
import com.anur.engine.api.constant.common.CommonApiConst
import com.anur.engine.api.constant.str.StrApiConst
import com.anur.engine.queryer.EngineDataQueryer
import com.anur.engine.result.common.DataHandler
import com.anur.engine.result.EngineResult
import com.anur.engine.result.common.EngineExecutor
import com.anur.engine.storage.entry.ByteBufferHanabiEntry
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

    private val logger = Debugger(EngineDataFlowControl.javaClass)

    fun commandInvoke(opera: Operation): EngineResult {
        val engineExecutor = EngineExecutor(EngineResult())
        val dataHandler = DataHandler(opera)
        engineExecutor.setDataHandler(dataHandler)

        var trxId = dataHandler.getTrxId()

        try {
            /*
             * common 操作比较特殊，它直接会有些特殊交互，比如开启一个事务，关闭一个事务等。
             */
            when (dataHandler.getCommandType()) {
                CommandTypeConst.COMMON -> {
                    when (dataHandler.getApi()) {
                        CommonApiConst.START_TRX -> {
                            logger.trace("事务 [${trxId}] 已经开启")
                            return engineExecutor.engineResult // TODO
                        }
                        CommonApiConst.COMMIT_TRX -> {
                            doCommit(trxId)
                            return engineExecutor.engineResult // TODO
                        }
                        CommonApiConst.ROLL_BACK -> {
                            throw RollbackException()
                        }
                    }
                }

                CommandTypeConst.STR -> {
                    when (dataHandler.getApi()) {
                        StrApiConst.SELECT -> {
                            doQuery(engineExecutor)
                        }
                        StrApiConst.DELETE -> {
                            doAcquire(engineExecutor, ByteBufferHanabiEntry.Companion.OperateType.DISABLE)
                        }
                        StrApiConst.SET -> {
                            doAcquire(engineExecutor, ByteBufferHanabiEntry.Companion.OperateType.ENABLE)
                        }
                        StrApiConst.SET_EXIST -> {
                            EngineDataQueryer.doQuery(engineExecutor)
                            engineExecutor.hanabiEntry()
                                    ?.also { engineExecutor.shotFailure() }
                                    ?: also {
                                        doAcquire(engineExecutor, ByteBufferHanabiEntry.Companion.OperateType.ENABLE)
                                    }
                        }
                        StrApiConst.SET_NOT_EXIST -> {
                            doQuery(engineExecutor)
                            engineExecutor.hanabiEntry()
                                    ?.also {
                                        doAcquire(engineExecutor, ByteBufferHanabiEntry.Companion.OperateType.ENABLE)
                                    }
                                    ?: also { engineExecutor.shotFailure() }
                        }
                        StrApiConst.SET_IF -> {
                            doQuery(engineExecutor)
                            val currentValue = engineExecutor.hanabiEntry()?.getValue()
                            val expectValue = dataHandler.extraParams[0]

                            if (expectValue == currentValue) {
                                doAcquire(engineExecutor, ByteBufferHanabiEntry.Companion.OperateType.ENABLE)
                            } else {
                                engineExecutor.shotFailure()
                            }
                        }
                    }
                }
            }

            if (dataHandler.shortTransaction) doCommit(trxId)
        } catch (e: Throwable) {
            logger.error("存储引擎执行出错，将执行回滚，原因 [$e.message]")
            e.printStackTrace()

            doRollBack(trxId)
            engineExecutor.exceptionCaught(e)
        }

        return engineExecutor.engineResult
    }

    private fun doQuery(engineExecutor: EngineExecutor) {
        EngineDataQueryer.doQuery(engineExecutor)
        logger.trace("事务 [${engineExecutor.getDataHandler().getTrxId()}] 对 key [${engineExecutor.getDataHandler().key}] 进行了查询操作" +
                " 数据位于 ${engineExecutor.engineResult.queryExecutorDefinition} 值为 ==> {${engineExecutor.hanabiEntry()}}")
    }

    /**
     * 进行事务控制与数据流转，通过无锁控制来进行阻塞与唤醒
     *
     * 如果拿到锁，则调用api，将数据插入 未提交部分(uc)
     */
    private fun doAcquire(engineExecutor: EngineExecutor, operateType: ByteBufferHanabiEntry.Companion.OperateType) {
        val dataHandler = engineExecutor.getDataHandler()
        val trxId = dataHandler.getTrxId()

        dataHandler.setOperateType(operateType)
        TrxFreeQueuedSynchronizer.acquire(trxId, dataHandler.key) {
            MemoryMVCCStorageUnCommittedPart.commonOperate(dataHandler)
        }
        logger.trace("事务 [${trxId}] 将 key [${engineExecutor.getDataHandler().key}] 设置为了新值")
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
