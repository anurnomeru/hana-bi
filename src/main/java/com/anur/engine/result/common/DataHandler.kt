package com.anur.engine.result.common

import com.anur.core.struct.base.Operation
import com.anur.engine.api.constant.CommandTypeConst
import com.anur.engine.api.constant.TransactionTypeConst
import com.anur.engine.storage.core.HanabiCommand
import com.anur.engine.storage.entry.ByteBufferHanabiEntry
import com.anur.engine.trx.manager.TrxManager
import com.anur.engine.trx.watermark.WaterMarkRegistry
import com.anur.engine.trx.watermark.WaterMarker
import com.anur.exception.UnexpectedException
import com.anur.exception.WaterMarkCreationException
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 *
 * 数据控制，需要用到很多数据，进入数据引擎是 Operation（持有一个 HanabiCommand），持久化后又是 HanabiEntry
 *
 * 那么如何节省内存，实际上就是这个类所做的事情
 */
class DataHandler(val operation: Operation) {

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

        val byteBuffer = operation.hanabiCommand.content

        // EXTRA: hanabiCommand 可以和  ByteBufferHanabiEntry 共享底层 byteBuffer
        byteBuffer.position(HanabiCommand.ValuesSizeOffset)
        val mainParamSize = byteBuffer.getInt()

        val from = HanabiCommand.CommandTypeOffset
        val to = HanabiCommand.ValuesSizeOffset + HanabiCommand.ValuesSizeLength + mainParamSize

//        println("byteBuffer total: ${byteBuffer.limit()} sliceFrom: ${from} sliceTo: ${to}")

        byteBuffer.position(from)
        byteBuffer.limit(to)

        val slice = byteBuffer.slice()

//        println("slice \t\t-> Position: ${slice.position()} limit: ${slice.limit()}")
//        println("byteBuffer \t-> Position: ${byteBuffer.position()} limit: ${byteBuffer.limit()}")

        byteBufferHanabiEntry = ByteBufferHanabiEntry(slice)
    }

    // ================================ ↓↓↓↓↓↓↓  直接访问 byteBuffer 拉取数据

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取 trxId
     */
    fun getTrxId() = operation.hanabiCommand.getTrxId()

    /**
     * 获取第一个参数
     */
    fun getValue() = byteBufferHanabiEntry.getValue()

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取 commandType
     */
    fun getCommandType() = CommandTypeConst.map(operation.hanabiCommand.getCommandType())

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取具体请求的 api
     */
    fun getApi() = operation.hanabiCommand.getApi()

    /**
     * 进行数据操作时必须设置这个值
     */
    private var operateType: ByteBufferHanabiEntry.Companion.OperateType? = null

    fun setOperateType(operateType: ByteBufferHanabiEntry.Companion.OperateType) {
        this.operateType = operateType

        val byteBuffer = byteBufferHanabiEntry.content
        byteBuffer.mark()

        byteBuffer.position(ByteBufferHanabiEntry.OperateTypeOffset)
        byteBuffer.put(operateType.byte)

        byteBuffer.reset()

//        println(byteBufferHanabiEntry.getExpectedSize())
//        println(byteBufferHanabiEntry.getValue())
//        println(byteBufferHanabiEntry.getOperateType())
    }

    @Synchronized
    fun genHanabiEntry(): ByteBufferHanabiEntry {
        if (operateType == null) {
            throw UnexpectedException("operateType 在进行非查询操作时必须指定！ 估计是代码哪里有 bug 导致没有指定！")
        }
        return byteBufferHanabiEntry
    }
}