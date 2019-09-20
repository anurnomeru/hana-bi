package com.anur.io.hanalog

import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.elect.operator.ElectOperator
import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.Operation
import com.anur.io.hanalog.log.LogManager

/**
 * Created by Anur IjuoKaruKas on 2019/7/14
 */
class TestAppendThenDiscard

fun main() {

    ElectOperator.getInstance().start()
//    try {
//        for (i in 0..99999) {
//            val operation = Operation(OperationTypeEnum.SETNX, "setAnur", "ToIjuoKaruKas".toByteArray())
//            LogManager.appendWhileClusterValid(operation)
//        }
//    } catch (e: Exception) {
//    }
//    LogManager.discardAfter(GenerationAndOffset(5, 5))
//
//    for (i in 0..99999) {
//        val operation = Operation(OperationTypeEnum.SETNX, "setAnur", "ToIjuoKaruKas".toByteArray())
//        LogManager.appendWhileClusterValid(operation)
//    }
    println()
}