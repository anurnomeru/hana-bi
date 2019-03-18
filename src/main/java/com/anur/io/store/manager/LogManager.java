package com.anur.io.store.manager;

import com.anur.config.InetSocketAddressConfigHelper;

/**
 * Created by Anur IjuoKaruKas on 2019/3/13
 *
 * 分段日志管理，这是真正的操作日志
 */
public class LogManager extends BaseManager {

    public static volatile LogManager INSTANCE;

    public static LogManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (LogManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new LogManager();
                }
            }
        }
        return INSTANCE;
    }

    public LogManager() {
        super(InetSocketAddressConfigHelper.getServerName() + "\\store\\aof\\log\\");
    }
}
