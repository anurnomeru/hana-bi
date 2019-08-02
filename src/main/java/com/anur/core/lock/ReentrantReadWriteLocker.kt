package com.anur.core.lock

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.Supplier

/**
 * Created by Anur IjuoKaruKas on 2019/7/10
 *
 * 提供一个基于读写锁的二次封装类
 */
open class ReentrantReadWriteLocker : ReentrantReadWriteLock() {

    private val readLock: ReadLock = readLock()

    private val writeLock: WriteLock = writeLock()

    private val switch = writeLock.newCondition()

    @Volatile
    private var switcher = 0

    fun switchOff() {
        switcher = 1
    }

    fun switchOn() {
        try {
            writeLock.lock()
            switcher = 0
            switch.signalAll()
        } finally {
            writeLock.unlock()
        }
    }

    /**
     * 提供一个统一的锁入口
     */
    fun writeLocker(doSomething: () -> Unit) {
        try {
            writeLock.lock()

            if (switcher > 0) {
                switch.await()
                error("喵喵喵！！！！！！！！！！！！")
            }

            doSomething.invoke()
        } finally {
            writeLock.unlock()
        }
    }

    /**
     * 提供一个统一的锁入口
     */
    fun <T> writeLockSupplier(supplier: Supplier<T>): T? {
        val t: T
        try {
            writeLock.lock()

            if (switcher > 0) {
                switch.await()
            }

            t = supplier.get()
        } finally {
            writeLock.unlock()
        }
        return t
    }

    /**
     * 提供一个统一的锁入口
     */
    fun <T> writeLockSupplierCompel(supplier: Supplier<T>): T {
        val t: T
        try {
            writeLock.lock()

            if (switcher > 0) {
                switch.await()
            }

            t = supplier.get()
        } finally {
            writeLock.unlock()
        }
        return t
    }


    /**
     * 提供一个统一的锁入口
     */
    fun <T> readLockSupplier(supplier: Supplier<T>): T? {
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
    fun <T> readLockSupplierCompel(supplier: Supplier<T>): T {
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
    fun readLocker(doSomething: () -> Unit) {
        try {
            readLock.lock()
            doSomething.invoke()
        } finally {
            readLock.unlock()
        }
    }

}