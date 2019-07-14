package com.anur.io.store

import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.Operation
import com.anur.io.store.log.LogManager

/**
 * Created by Anur IjuoKaruKas on 2019/7/14
 */
class TestAppendThenDiscard

fun main() {
    try {
        for (i in 0..99999) {
            val operation = Operation(OperationTypeEnum.SETNX, "setAnur", "ToIjuoKaruKas")
            LogManager.append(operation)
        }
    } catch (e: Exception) {
    }

}