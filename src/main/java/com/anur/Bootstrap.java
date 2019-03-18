package com.anur;

import com.anur.core.coordinate.CoordinateServerOperator;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.ElectServerOperator;
import com.anur.io.store.manager.LogManager;
/**
 * Created by Anur IjuoKaruKas on 2019/3/13
 */
public class Bootstrap {

    public static void main(String[] args) {

        /**
         * 初始化日志管理
         */
        LogManager logManager = LogManager.getINSTANCE();

        /**
         * 启动协调服务器
         */
        CoordinateServerOperator.getInstance()
                                .start();

        /**
         * 启动选举服务器，没什么主要的操作，这个服务器主要就是应答选票以及应答成为 Flower 用
         */
        ElectServerOperator.getInstance()
                           .start();

        /**
         * 启动选举客户端，初始化各种投票用的信息，以及启动成为候选者的定时任务
         */
        ElectOperator.getInstance()
                     .resetGenerationAndOffset(logManager.getInitial())
                     .start();
    }
}
