package com.anur.core.coordinate.recovery;

/**
 * Created by Anur IjuoKaruKas on 4/9/2019
 *
 * 当服务器挂机或者不正常
 */
public class ClusterRecoveryManager {

    private volatile static ClusterRecoveryManager INSTANCE;

    public static ClusterRecoveryManager getInstance() {
        if (INSTANCE == null) {
            synchronized (ClusterRecoveryManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ClusterRecoveryManager();
                }
            }
        }
        return INSTANCE;
    }


}
