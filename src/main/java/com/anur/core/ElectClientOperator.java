package com.anur.core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.util.ShutDownHooker;

/**
 * Created by Anur IjuoKaruKas on 2/1/2019
 *
 * 选举客户端操作类
 */
public class ElectClientOperator {

    private static Logger logger = LoggerFactory.getLogger(ElectClientOperator.class);

    private volatile static ElectClientOperator INSTANCE;

    /**
     * 关闭本服务的钩子
     */
    private ShutDownHooker serverShutDownHooker;

    /**
     * 启动latch
     */
    private CountDownLatch initialLatch = new CountDownLatch(2);

    /**
     * 是否正处于选举之中
     */
    private Semaphore electState;

    public static ElectClientOperator getInstance() {
        if (INSTANCE == null) {
            synchronized (ElectServerOperator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ElectClientOperator();
                }
            }
        }
        return INSTANCE;
    }


}
