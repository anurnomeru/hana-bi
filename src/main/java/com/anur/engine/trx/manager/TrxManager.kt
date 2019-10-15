package com.anur.engine.trx.manager

import com.anur.core.lock.rentrant.ReentrantReadWriteLocker
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Supplier


/**
 * Created by Anur IjuoKaruKas on 2019/10/15
 *
 * 事务管理器
 */
object TrxManager {

    /** 显式锁 */
    private val appendLock = ReentrantReadWriteLocker()

    private val nowTrx: AtomicLong = AtomicLong(0)

    fun allocate(): Long {
        appendLock.readLockSupplier(Supplier {

        })
    }
}