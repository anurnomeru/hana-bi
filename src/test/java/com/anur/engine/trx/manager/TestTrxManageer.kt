package com.anur.engine.trx.manager


/**
 * Created by Anur IjuoKaruKas on 2019/10/22
 */
fun main() {
    for (i in 0 until 123456) {
        TrxManager.allocate()
    }
    println(TrxManager.minTrx())

    for (i in 0 until 666) {
        TrxManager.releaseTrx(TrxManager.StartTrx + i.toLong())
    }
    println(TrxManager.minTrx())
    println(TrxManager.minTrx() - (TrxManager.StartTrx))
}