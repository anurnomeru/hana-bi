package com.anur.util;

import java.util.Optional;
import java.util.ResourceBundle;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 */
public class ConfigHelper {

    protected volatile static ResourceBundle RESOURCE_BUNDLE;

    private static Lock READ_LOCK;

    private static Lock WRITE_LOCK;

    static {
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        READ_LOCK = readWriteLock.readLock();
        WRITE_LOCK = readWriteLock.writeLock();
        RESOURCE_BUNDLE = ResourceBundle.getBundle("application");
    }

    /**
     * 根据key获取某个配置
     */
    protected static <T> T getConfig(String key, Function<String, T> transfer) {
        T t;
        try {
            READ_LOCK.lock();
            t = Optional.of(key)
                        .map(s -> RESOURCE_BUNDLE.getString(s))
                        .map(transfer)
                        .orElseThrow(() -> new ApplicationConfigException("读取application.properties配置异常，异常项目：" + key));
        } finally {
            READ_LOCK.unlock();
        }
        return t;
    }

    /**
     * 刷新配置
     */
    public static void refresh() {
        try {
            WRITE_LOCK.lock();
            RESOURCE_BUNDLE = ResourceBundle.getBundle("application");
        } finally {
            WRITE_LOCK.unlock();
        }
    }

    public static class ApplicationConfigException extends HanabiException {

        public ApplicationConfigException(String message) {
            super(message);
        }
    }
}
