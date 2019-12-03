package com.anur.engine.result.common

import com.anur.core.struct.base.Operation
import com.anur.engine.api.constant.CommandTypeConst
import com.anur.engine.api.constant.TransactionTypeConst
import com.anur.engine.storage.core.HanabiCommand
import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.trx.manager.TrxManager
import com.anur.engine.trx.watermark.WaterMarkRegistry
import com.anur.engine.trx.watermark.WaterMarker
import com.anur.exception.UnexpectedException
import com.anur.exception.WaterMarkCreationException

/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 */
class ParameterHandler(val operation: Operation) {

    /**
     * 进行数据操作时必须设置这个值
     */
    var operateType: HanabiEntry.Companion.OperateType? = null

    private var hanabiEntry: HanabiEntry? = null

    @Synchronized
    fun genHanabiEntry(): HanabiEntry {
        hanabiEntry ?: also {
            if (operateType == null) {
                throw UnexpectedException("operateType 在进行非查询操作时必须指定！ 估计是代码哪里有bug！")
            }
            hanabiEntry = HanabiEntry(storageType, values[0], operateType!!)
        }
        return hanabiEntry!!
    }

    /////////////////////////////////////////// init

    val key = operation.key

    val hanabiCommand = operation.hanabiCommand

    val trxId = hanabiCommand.getTrxId()

    val values = hanabiCommand.getValues()

    val storageType = CommandTypeConst.map(hanabiCommand.getType())

    val api = hanabiCommand.getApi()

    /**
     * 如果是短事务，操作完就直接提交
     *
     * 如果是长事务，则如果没有激活过事务，需要进行事务的激活(创建快照)
     */
    val shortTransaction = TransactionTypeConst.map(hanabiCommand.getTransactionType()) == TransactionTypeConst.SHORT

    /**
     * 短事务不需要快照，长事务则是有快照就用快照，没有就创建一个快照
     */
    private var waterMarker: WaterMarker

    fun getWaterMarker(): WaterMarker = waterMarker

    init {
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
    }
}