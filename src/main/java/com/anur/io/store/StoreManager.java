package com.anur.io.store;

import com.anur.io.store.manager.LogManager;

/**
 * Created by Anur IjuoKaruKas on 2019/3/18
 *
 * 管理所有日志
 */
public class StoreManager {

    private static StoreManager INSTANCE;

    /**
     * 初始化所有日志
     *
     * 1、初始化 LogManager，加载所有的操作日志
     */
    public void initial() {
        LogManager logManager = LogManager.getINSTANCE();
    }
}
