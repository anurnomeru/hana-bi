package com.anur.io.store;

import com.anur.io.store.log.Log;
/**
 * Created by Anur IjuoKaruKas on 2019/3/18
 *
 * 管理所有日志
 */
public class StoreManager {

    public void initial() {
        LogManager logManager = LogManager.getINSTANCE();
        Log log = logManager.activeLog();

        PreLogManager preLogManager = PreLogManager.getINSTANCE();

    }
}
