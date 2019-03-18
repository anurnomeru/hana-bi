package com.anur.io.store.manager;

import com.anur.config.InetSocketAddressConfigHelper;

/**
 * Created by Anur IjuoKaruKas on 2019/3/18
 *
 * 预日志管理
 */
public class PreLogManager extends BaseManager {

    public static volatile PreLogManager INSTANCE;

    public static PreLogManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (PreLogManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new PreLogManager();
                }
            }
        }
        return INSTANCE;
    }

    private PreLogManager() {
        super(InetSocketAddressConfigHelper.getServerName() + "\\store\\aof\\prelog\\");
    }
}
