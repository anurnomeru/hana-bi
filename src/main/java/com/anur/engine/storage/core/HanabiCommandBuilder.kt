package com.anur.engine.storage.core

import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.Operation
import com.anur.engine.EngineDataFlowControl
import com.anur.engine.api.constant.StorageTypeConst
import com.anur.engine.api.constant.TransactionTypeConst
import com.anur.engine.api.constant.common.CommonApiConst
import com.anur.engine.api.constant.str.StrApiConst
import com.anur.engine.trx.manager.TrxAllocator


/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 构造专门用于请求数据引擎的构造器，不传入事务id，则默认为短事务
 */
object HanabiCommandBuilder {

    fun select(key: String, trx: Long? = null): Operation {
        return Operation(OperationTypeEnum.COMMAND, key,
                HanabiCommand.generator(
                        trx ?: TrxAllocator.allocate(),
                        trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,
                        StorageTypeConst.STR, StrApiConst.SELECT))
    }

    fun insert(key: String, value: String, trx: Long? = null): Operation {
        return Operation(OperationTypeEnum.COMMAND, key,
                HanabiCommand.generator(
                        trx ?: TrxAllocator.allocate(),
                        trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,
                        StorageTypeConst.STR, StrApiConst.INSERT, value))
    }

    fun update(key: String, value: String, trx: Long? = null): Operation {
        return Operation(OperationTypeEnum.COMMAND, key,
                HanabiCommand.generator(
                        trx ?: TrxAllocator.allocate(),
                        trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,
                        StorageTypeConst.STR, StrApiConst.UPDATE, value))
    }

    fun delete(key: String, trx: Long? = null): Operation {
        return Operation(OperationTypeEnum.COMMAND, key,
                HanabiCommand.generator(
                        trx ?: TrxAllocator.allocate(),
                        trx?.let { TransactionTypeConst.LONG } ?: TransactionTypeConst.SHORT,
                        StorageTypeConst.STR, StrApiConst.DELETE))
    }

    fun commit(trx: Long): Operation {
        return Operation(OperationTypeEnum.COMMAND, "",
                HanabiCommand.generator(
                        trx,
                        TransactionTypeConst.LONG,
                        StorageTypeConst.COMMON, CommonApiConst.COMMIT_TRX))
    }
}