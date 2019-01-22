package com.anur.core.lock;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 */
public abstract class ReentrantLocker {

    private ReentrantLock reentrantLock;

    public ReentrantLocker() {
        this.reentrantLock = new ReentrantLock();
    }

    /**
     * 提供一个统一的锁入口
     */
    protected <T> T lockSupplier(Supplier<T> supplier) {
        T t;
        try {
            reentrantLock.lock();
            t = supplier.get();
        } finally {
            reentrantLock.unlock();
        }
        return t;
    }
}
