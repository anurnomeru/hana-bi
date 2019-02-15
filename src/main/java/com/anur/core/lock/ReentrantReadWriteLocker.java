package com.anur.core.lock;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Supplier;
/**
 * Created by Anur IjuoKaruKas on 2/15/2019
 */
public abstract class ReentrantReadWriteLocker {

    private ReadLock readLock;

    private WriteLock writeLock;

    public ReentrantReadWriteLocker() {
        ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        this.readLock = reentrantReadWriteLock.readLock();
        this.writeLock = reentrantReadWriteLock.writeLock();
    }

    /**
     * 提供一个统一的锁入口
     */
    protected <T> T readLockSupplier(Supplier<T> supplier) {
        T t;
        try {
            readLock.lock();
            t = supplier.get();
        } finally {
            readLock.unlock();
        }
        return t;
    }

    /**
     * 提供一个统一的锁入口
     */
    protected <T> T writeLockSupplier(Supplier<T> supplier) {
        T t;
        try {
            writeLock.lock();
            t = supplier.get();
        } finally {
            writeLock.unlock();
        }
        return t;
    }
}
