package com.anur.core.lock

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.Supplier

/**
 * Created by Anur IjuoKaruKas on 2019/7/10
 */
open class ReentrantReadWriteLocker {

    private val readLock: ReentrantReadWriteLock.ReadLock

    private val writeLock: ReentrantReadWriteLock.WriteLock

    init {
        val reentrantReadWriteLock = ReentrantReadWriteLock()
        this.readLock = reentrantReadWriteLock.readLock()
        this.writeLock = reentrantReadWriteLock.writeLock()
    }

    /**
     * 提供一个统一的锁入口
     */
    protected fun <T> readLockSupplier(supplier: Supplier<T>): T? {
        val t: T
        try {
            readLock.lock()
            t = supplier.get()
        } finally {
            readLock.unlock()
        }
        return t
    }
    /**
     * 提供一个统一的锁入口
     */
    protected fun <T> readLockSupplierCompel(supplier: Supplier<T>): T {
        val t: T
        try {
            readLock.lock()
            t = supplier.get()
        } finally {
            readLock.unlock()
        }
        return t
    }

    /**
     * 提供一个统一的锁入口
     */
    protected fun <T> writeLockSupplier(supplier: Supplier<T>): T? {
        val t: T
        try {
            writeLock.lock()
            t = supplier.get()
        } finally {
            writeLock.unlock()
        }
        return t
    }

    /**
     * 提供一个统一的锁入口
     */
    protected fun <T> writeLockSupplierCompel(supplier: Supplier<T>): T {
        val t: T
        try {
            writeLock.lock()
            t = supplier.get()
        } finally {
            writeLock.unlock()
        }
        return t
    }

    /**
     * 提供一个统一的锁入口
     */
    protected fun readLocker(doSomething: () -> Unit) {
        try {
            readLock.lock()
            doSomething.invoke()
        } finally {
            readLock.unlock()
        }
    }

    /**
     * 提供一个统一的锁入口
     */
    protected fun writeLocker(doSomething: () -> Unit) {
        try {
            writeLock.lock()
            doSomething.invoke()
        } finally {
            writeLock.unlock()
        }
    }
}