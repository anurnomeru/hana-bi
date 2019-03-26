package com.anur.core;

/**
 * Created by Anur IjuoKaruKas on 2019/3/26
 */
public class ConsistentManager {

    private static volatile ConsistentManager INSTANCE;

    public ConsistentManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (ConsistentManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ConsistentManager();
                }
            }
        }
        return INSTANCE;
    }
}
