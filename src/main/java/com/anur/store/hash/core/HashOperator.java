package com.anur.store.hash.core;

import java.util.HashMap;
import java.util.Objects;
import com.anur.core.lock.ReentrantReadWriteLocker;

/**
 * Created by Anur IjuoKaruKas on 2/15/2019
 *
 * 用于在内存中存储 k - v
 */
public class HashOperator extends ReentrantReadWriteLocker {

    private HashMap<String, String> hashMap;

    private volatile HashOperator INSTANCE;

    public HashOperator getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (this) {
                if (INSTANCE == null) {
                    INSTANCE = new HashOperator();
                }
            }
        }
        return INSTANCE;
    }

    private HashOperator() {
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
