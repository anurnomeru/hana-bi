package com.anur.core.lock.rentrant

import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier

/**
 * Created by Anur IjuoKaruKas on 2019/7/10
 */
open class ReentrantLocker {
    private val reentrantLock: ReentrantLock = ReentrantLock()

    /**
     * 提供一个统一的锁入口
     */
    protected fun <T> lockSupplier(supplier: Supplier<T>): T? {
        val t: T
        try {
            reentrantLock.lock()
            t = supplier.get()
        } finally {
            reentrantLock.unlock()
        }
        return t
    }

    /**
     * 提供一个统一的锁入口
     */
    protected fun <T> lockSupplierCompel(supplier: Supplier<T>): T {
        val t: T
        try {
            reentrantLock.lock()
            t = supplier.get()
        } finally {
            reentrantLock.unlock()
        }
        return t
    }

    /**
     * 提供一个统一的锁入口
     */
    protected fun lockSupplier(doSomething: () -> Unit) {
        try {
            reentrantLock.lock()
            doSomething.invoke()
        } finally {
            reentrantLock.unlock()
        }
    }
}