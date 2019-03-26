package com.anur.core;

import com.anur.core.elect.ElectOperator;
/**
 * Created by Anur IjuoKaruKas on 2019/3/26
 *
 * 日志一致性控制器
 */
public class ConsistentManager {

    private static volatile ConsistentManager INSTANCE;

    public static ConsistentManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (ConsistentManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ConsistentManager();
                }
            }
        }
        return INSTANCE;
    }

    public ConsistentManager() {
        ElectOperator.getInstance().registerWhenClusterValid(() -> System.out.println("lalalalalalaalalallaalla"));
    }
}
