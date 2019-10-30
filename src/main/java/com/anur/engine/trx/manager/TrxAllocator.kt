package com.anur.engine.trx.manager


/**
 * Created by Anur IjuoKaruKas on 2019/10/23
 *
 * 专门用于 leader 来生成事务id
 */
object TrxAllocator {

    const val StartTrx: Long = Long.MIN_VALUE
    private var nowTrx: Long = StartTrx

    /**
     * 申请一个递增的事务id
     */
    fun allocate(): Long {
        val trx = nowTrx
        if (trx == Long.MAX_VALUE) {
            nowTrx = Long.MIN_VALUE
        } else {
            nowTrx++
        }
        return trx
    }
}