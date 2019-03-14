package com.anur.store.core;

import java.util.HashMap;
import java.util.Objects;
import com.anur.core.lock.ReentrantReadWriteLocker;

/**
 * Created by Anur IjuoKaruKas on 2/15/2019
 *
 * 用于在内存中存储 k - v
 */
public class StoreOperator extends ReentrantReadWriteLocker {

    private HashMap<String, String> hashMap;

    private static volatile StoreOperator INSTANCE;

    public static StoreOperator getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (StoreOperator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new StoreOperator();
                }
            }
        }
        return INSTANCE;
    }

    private StoreOperator() {
        this.hashMap = new HashMap<>();
    }

    public String get(String key) {
        return this.readLockSupplier(() -> hashMap.get(key));
    }

    public boolean setNx(String key, String val, boolean nx) {
        return this.writeLockSupplier(() -> {
            if (nx) {
                return Objects.equals(hashMap.putIfAbsent(key, val), val);
            } else {
                return Objects.equals(hashMap.put(key, val), val);
            }
        });
    }

    /**
     * 移除成功 返回 false
     */
    public boolean rm(String key) {
        return this.writeLockSupplier(() ->
            hashMap.remove(key) != null
        );
    }
}
