package com.anur.io.store;

import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.exception.HanabiException;
import com.anur.io.store.manager.LogManager;
import com.anur.io.store.manager.PreLogManager;

/**
 * Created by Anur IjuoKaruKas on 2019/3/18
 *
 * 管理所有日志
 */
public class StoreManager {

    public void initial() {
        LogManager logManager = LogManager.getINSTANCE();
        GenerationAndOffset logGAA = logManager.getInitial();

        PreLogManager preLogManager = PreLogManager.getINSTANCE();
        GenerationAndOffset preLogGAA = preLogManager.getInitial();

        int result = logGAA.compareTo(preLogGAA);
        if (result < 0) {
            throw new HanabiException("预日志进度不应该比操作日志慢");
        } else if (result == 0) {
            // 不需要做任何操作
        } else {
            // 日志同步
        }
    }
}
