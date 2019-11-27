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
    val trx1 = TrxAllocator.allocate()
    insert("Anur", "Version 1", trx1)

    val trx2 = TrxAllocator.allocate()
    insert("Anur", "zzz", trx2)

    select("Anur") // 由于隔离性，查不到
    select("Anur", trx2)// 由于隔离性，查不到

    commit(trx1)

    select("Anur") // Version 1
    select("Anur", trx2)// 由于 trx1已经提交，所以trx2进入了 未提交部分，所以能查到值为 zzz

    insert("Anur", "zzzz")// 阻塞
    insert("Anur", "zzzzzzz")// 阻塞

    commit(trx2)
    select("Anur")// zzzzzzz
    delete("Anur")
    select("Anur")// 空
    Thread.sleep(10000)
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