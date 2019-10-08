package com.anur.engine.api

import com.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import com.anur.engine.api.common.ApiDispatcher
import com.anur.engine.api.common.StrApiDispatcher
import com.anur.engine.api.common.type.StorageTypeConst
import com.anur.exception.UnSupportedApiException
import java.util.LinkedList
import java.util.concurrent.CountDownLatch
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
    val s = HashSet<String>()
    var currentTimeMillis1 = System.currentTimeMillis()

    val times = 5000000
    val cdl = CountDownLatch(times)

    for (i in 0 until times) {
        val trxId = i.toLong() / 5
        s.add(trxId.toString())
        val key = random.nextInt(5000000).toString()
        TrxFreeQueuedSynchronizer.acquire(trxId, key) {}
    }

    cdl.await()
    var currentTimeMillis2 = System.currentTimeMillis()

    for (l in s) {
        TrxFreeQueuedSynchronizer.release(l.toLong()) {}
    }

    println("${(currentTimeMillis2 - currentTimeMillis1)} ms")
    println("${(System.currentTimeMillis() - currentTimeMillis2)} ms")

    Thread.sleep(100000)
}