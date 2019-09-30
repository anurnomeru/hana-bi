package com.anur.engine.trx.lock.entry


/**
 * Created by Anur IjuoKaruKas on 2019/9/30
 */
class Releaser(val trxId: Long, val doWhileCommit: () -> Unit)