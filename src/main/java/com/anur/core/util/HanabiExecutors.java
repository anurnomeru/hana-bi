package com.anur.core.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by Anur IjuoKaruKas on 2/1/2019
 */
public class HanabiExecutors {

    /**
     * 统一管理的线程池
     */
    private static ExecutorService POOL = java.util.concurrent.Executors.newFixedThreadPool(Integer.MAX_VALUE);

    public static Future<?> submit(Runnable runnable) {
        return POOL.submit(runnable);
    }

    public static <T> Future<T> submit(Callable<T> task) {
        return POOL.submit(task);
    }
}
