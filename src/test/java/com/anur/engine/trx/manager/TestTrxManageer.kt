package com.anur.engine.trx.manager

import java.lang.StringBuilder


/**
 * Created by Anur IjuoKaruKas on 2019/10/22
 */
fun main() {
    val currentTimeMillis = System.currentTimeMillis()

    val intervalTest = 10000L

    for (i in 0..intervalTest) {
        TrxManager.activateTrx(TrxManager.allocateTrx(), false)
    }
    val minBefore = TrxManager.lowWaterMark()

    for (i in TrxAllocator.StartTrx until TrxAllocator.StartTrx + intervalTest) {
        TrxManager.releaseTrx(i)
    }
    println("Before : $minBefore")
    println("Now : ${TrxManager.lowWaterMark()}")

    println(TrxManager.lowWaterMark() - minBefore)
}


fun toBinaryStr(long: Long) {
//    println(toBinaryStrIter(long, 63, StringBuilder()).toString())
}

fun toBinaryStrIter(long: Long, index: Int, appender: StringBuilder): StringBuilder {
    if (index == -1) {
        return appender
    } else {
        var mask = 1L shl index
        if (mask and long == mask) {
            appender.append("1")
        } else {
            appender.append("0")
        }
        return toBinaryStrIter(long, index - 1, appender)
    }
}