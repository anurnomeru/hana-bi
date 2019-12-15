package com.anur.engine.processor

import com.anur.core.log.Debugger
import com.anur.core.struct.base.Operation
import com.anur.engine.api.constant.CommandTypeConst
import com.anur.engine.api.constant.TransactionTypeConst
import com.anur.engine.common.core.HanabiCommand
import com.anur.engine.common.entry.ByteBufferHanabiEntry
import com.anur.engine.trx.manager.TrxManager
import com.anur.engine.trx.watermark.WaterMarkRegistry
import com.anur.engine.trx.watermark.WaterMarker
import com.anur.exception.UnexpectedException
import com.anur.exception.WaterMarkCreationException

/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 *
 * 数据控制，需要用到很多数据，进入数据引擎是 Operation（持有一个 HanabiCommand），持久化后又是 HanabiEntry
 *
 * 那么如何节省内存，实际上就是这个类所做的事情
 */
class DataHandler(val operation: Operation) {

    companion object {
        val logger = Debugger(DataHandler.javaClass)
    }

    /////////////////////////////////////////// init

    /**
     * 从 Operation 中直接获取 key
     */
    val key = operation.key

    /**
     * 如果是短事务，操作完就直接提交
     *
     * 如果是长事务，则如果没有激活过事务，需要进行事务的激活(创建快照)
     */
    val shortTransaction = TransactionTypeConst.map(operation.hanabiCommand.getTransactionType()) == TransactionTypeConst.SHORT

    /**
     * 短事务不需要快照，长事务则是有快照就用快照，没有就创建一个快照
     */
    var waterMarker: WaterMarker

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取额外的参数（第一个参数以外的参数）
     */
    val extraParams: MutableList<String>

    /**
     * hanabi Entry，里面存储着数据本身的类型（str，还是其他的，是否被删除，以及 value）
     */
    private val byteBufferHanabiEntry: ByteBufferHanabiEntry

    init {
        val trxId = getTrxId()
        waterMarker = WaterMarkRegistry.findOut(trxId)
        // 如果没有水位快照，代表此事务从未激活过，需要去激活一下
        if (waterMarker == WaterMarker.NONE) {
            if (shortTransaction) {
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

        // 取出额外参数
        extraParams = operation.hanabiCommand.getExtraValues()
        byteBufferHanabiEntry = operation.hanabiCommand.getHanabiEntry()

        // 这后面的内存可以释放掉了
        operation.hanabiCommand.content.limit(HanabiCommand.TransactionSignOffset)
    }

    // ================================ ↓↓↓↓↓↓↓  直接访问 byteBuffer 拉取数据

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取 trxId
     */
    fun getTrxId() = operation.hanabiCommand.getTrxId()

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取 commandType
     */
    fun getCommandType() = CommandTypeConst.map(operation.hanabiCommand.getCommandType())

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取具体请求的 api
     */
    fun getApi() = operation.hanabiCommand.getApi()

    /**
     * 除了select操作，其余操作必须指定这个
     */
    fun setOperateType(operateType: ByteBufferHanabiEntry.Companion.OperateType) {
        byteBufferHanabiEntry.setOperateType(operateType)
    }

    @Synchronized
    fun genHanabiEntry(): ByteBufferHanabiEntry {
        if (!byteBufferHanabiEntry.operateTypeSet) {
            throw UnexpectedException("operateType 在进行非查询操作时必须指定！ 估计是代码哪里有 bug 导致没有指定！")
        }
        return byteBufferHanabiEntry
    }

    /**
     * 释放内存
     */
    fun destroy() {

    }
}