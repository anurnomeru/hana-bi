package com.anur.engine

import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.Operation
import com.anur.engine.api.constant.StorageTypeConst
import com.anur.engine.api.constant.TransactionTypeConst
import com.anur.engine.api.constant.str.StrApiConst
import com.anur.engine.storage.core.HanabiCommand
import com.anur.engine.trx.manager.TrxAllocator


/**
 * Created by Anur IjuoKaruKas on 2019/10/31
 */
fun main() {
    insert("Anur", "zzzz")
    insert("Anur", "zzzzzzz")
    insert("123", "zxcv")
    Thread.sleep(100000)
}

fun insert(key: String, value: String) {
    val oper = Operation(OperationTypeEnum.COMMAND, key,
            HanabiCommand.generator(TrxAllocator.allocate(), TransactionTypeConst.SHORT, StorageTypeConst.STR, StrApiConst.INSERT, value))
    EngineDataFlowControl.commandInvoke(oper)
}