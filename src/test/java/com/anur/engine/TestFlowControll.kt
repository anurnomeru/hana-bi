package com.anur.engine

import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.Operation
import com.anur.engine.api.constant.StorageTypeConst
import com.anur.engine.api.constant.TransactionTypeConst
import com.anur.engine.api.constant.common.CommonApiConst
import com.anur.engine.api.constant.str.StrApiConst
import com.anur.engine.storage.core.HanabiCommand
import com.anur.engine.trx.manager.TrxAllocator


/**
 * Created by Anur IjuoKaruKas on 2019/10/31
 */
fun main() {
    val longTrxForInsert = TrxAllocator.allocate()
    insert("Anur", "Version 1", longTrxForInsert)

    val longTrxForQuery = TrxAllocator.allocate()
    insert("zzz", "zzz", longTrxForQuery)

    select("Anur")
    select("Anur", longTrxForQuery)

    commit(longTrxForInsert)

    select("Anur")
    select("Anur", longTrxForQuery)

    insert("Anur", "zzzz")
    insert("Anur", "zzzzzzz")
    select("Anur")
    delete("Anur")
    select("Anur")
    Thread.sleep(100000)
}

fun select(key: String, trx: Long? = null) {
    val oper = Operation(OperationTypeEnum.COMMAND, key,
            HanabiCommand.generator(
                    trx ?: TrxAllocator.allocate(),
                    trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,

                    StorageTypeConst.STR, StrApiConst.SELECT))
    EngineDataFlowControl.commandInvoke(oper)
}

fun insert(key: String, value: String, trx: Long? = null) {
    val oper = Operation(OperationTypeEnum.COMMAND, key,
            HanabiCommand.generator(
                    trx ?: TrxAllocator.allocate(),
                    trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,

                    StorageTypeConst.STR, StrApiConst.INSERT, value))
    EngineDataFlowControl.commandInvoke(oper)
}

fun delete(key: String, trx: Long? = null) {
    val oper = Operation(OperationTypeEnum.COMMAND, key,
            HanabiCommand.generator(
                    trx ?: TrxAllocator.allocate(),
                    trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,

                    StorageTypeConst.STR, StrApiConst.DELETE))
    EngineDataFlowControl.commandInvoke(oper)
}

fun commit(trx: Long) {
    val oper = Operation(OperationTypeEnum.COMMAND, "",
            HanabiCommand.generator(
                    trx,
                    TransactionTypeConst.LONG,
                    StorageTypeConst.COMMON, CommonApiConst.COMMIT_TRX))
    EngineDataFlowControl.commandInvoke(oper)
}