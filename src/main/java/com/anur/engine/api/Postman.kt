package com.anur.engine.api

import com.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import com.anur.engine.api.common.ApiDispatcher
import com.anur.engine.api.common.StrApiDispatcher
import com.anur.engine.api.common.type.StorageTypeConst
import com.anur.exception.UnSupportedApiException
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

    val random = Random(10)
    val s = mutableSetOf<Long>()
    var currentTimeMillis1 = System.currentTimeMillis()


    for (i in 0 until 300000) {
        val trxId = i.toLong() / 100
        s.add(trxId)
        val key = random.nextInt(100000).toString()
        TrxFreeQueuedSynchronizer.acquire(trxId, key) {}
    }


    var currentTimeMillis2 = System.currentTimeMillis()
    for (l in s) {
        TrxFreeQueuedSynchronizer.release(l) {}
    }

    println("${(System.currentTimeMillis() - currentTimeMillis1)} ms")
    println("${(System.currentTimeMillis() - currentTimeMillis2)} ms")

    Thread.sleep(100000)
}