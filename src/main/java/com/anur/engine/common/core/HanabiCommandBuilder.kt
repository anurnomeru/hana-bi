package com.anur.engine.common.core

import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.Operation
import com.anur.engine.api.constant.CommandTypeConst
import com.anur.engine.api.constant.TransactionTypeConst
import com.anur.engine.api.constant.common.CommonApiConst
import com.anur.engine.api.constant.str.StrApiConst
import com.anur.engine.trx.manager.TrxManager


/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 构造专门用于请求数据引擎的构造器，不传入事务id，则默认为短事务
 */
object HanabiCommandBuilder {

    fun select(key: String, trxId: Long? = null): Operation {
        return Operation(OperationTypeEnum.COMMAND, key,
                HanabiCommand.generator(
                        trxId ?: TrxManager.allocateTrx(),
                        trxId?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,
                        CommandTypeConst.STR, StrApiConst.SELECT))
    }

    fun delete(key: String, trx: Long? = null): Operation {
        return Operation(OperationTypeEnum.COMMAND, key,
                HanabiCommand.generator(
                        trx ?: TrxManager.allocateTrx(),
                        trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,
                        CommandTypeConst.STR, StrApiConst.DELETE))
    }

    fun set(key: String, value: String, trx: Long? = null): Operation {
        return Operation(OperationTypeEnum.COMMAND, key,
                HanabiCommand.generator(
                        trx ?: TrxManager.allocateTrx(),
                        trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,
                        CommandTypeConst.STR, StrApiConst.SET, value))
    }

    fun setIfNotExist(key: String, value: String, trx: Long? = null): Operation {
        return Operation(OperationTypeEnum.COMMAND, key,
                HanabiCommand.generator(
                        trx ?: TrxManager.allocateTrx(),
                        trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,
                        CommandTypeConst.STR, StrApiConst.SET_NOT_EXIST, value))
    }

    fun setIfExist(key: String, value: String, trx: Long? = null): Operation {
        return Operation(OperationTypeEnum.COMMAND, key,
                HanabiCommand.generator(
                        trx ?: TrxManager.allocateTrx(),
                        trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,
                        CommandTypeConst.STR, StrApiConst.SET_EXIST, value))
    }

    fun setIf(key: String, value: String, expect: String, trx: Long? = null): Operation {
        return Operation(OperationTypeEnum.COMMAND, key,
                HanabiCommand.generator(
                        trx ?: TrxManager.allocateTrx(),
                        trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,
                        CommandTypeConst.STR, StrApiConst.SET_IF, value, expect))
    }

    fun commit(trx: Long): Operation {
        return Operation(OperationTypeEnum.COMMAND, "",
                HanabiCommand.generator(
                        trx,
                        TransactionTypeConst.LONG,
                        CommandTypeConst.COMMON, CommonApiConst.COMMIT_TRX))
    }
}