package com.anur.engine.trx.lock.entry


/**
 * Created by Anur IjuoKaruKas on 2019/9/30
 */
class Acquirer(val trxId: Long, val key: String, val whatEverDo: () -> Unit) {
    companion object {
        val ShutDownSign = Acquirer(Long.MIN_VALUE, "shut-down") {}
    }
}