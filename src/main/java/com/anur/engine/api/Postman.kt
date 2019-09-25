package com.anur.engine.api

import com.anur.core.lock.free.TrxFreeQueuedLocker
import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.Operation
import com.anur.engine.api.common.ApiDispatcher
import com.anur.engine.api.common.StrApiDispatcher
import com.anur.engine.api.common.api.StrApiConst
import com.anur.engine.api.common.base.EngineRequest
import com.anur.engine.api.common.type.StorageTypeConst
import com.anur.engine.storage.core.HanabiEntry
import com.anur.exception.UnSupportedApiException
import com.anur.util.HanabiExecutors
import kotlin.random.Random


/**
 * Created by Anur IjuoKaruKas on 2019/9/25
 */
object Postman {

    fun disPatchType(type: Byte): ApiDispatcher {
        return when (type) {
            StorageTypeConst.str -> {
                return StrApiDispatcher
            }
            else -> {
                throw UnSupportedApiException()
            }
        }
    }
}

fun main() {

    val operation = Operation(OperationTypeEnum.COMMAND, "AnurKey",
        HanabiEntry.generator(99, StorageTypeConst.str, StrApiConst.insert, "HanabiValue-中文-"))

    ////////


    val hanabiEntry = operation.hanabiEntry

    val random = Random(10)

    val s = mutableSetOf<Long>()

    for (i in 0 until 20) {
        val trxId = i.toLong() / 3
        s.add(trxId)
        val key = random.nextInt(10).toString()
        TrxFreeQueuedLocker.acquire(trxId, key) {
            Postman
                .disPatchType(hanabiEntry.getType())
                .api(hanabiEntry.getApi())
                .invoke(EngineRequest(trxId, key, hanabiEntry.getValue()))

            // 模拟任务执行时间很长
        }
    }

    for (l in s) {
        TrxFreeQueuedLocker.commit(l) {}
    }

    Thread.sleep(100000)
}