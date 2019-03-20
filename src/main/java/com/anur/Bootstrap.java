package com.anur;

import com.anur.core.coordinate.CoordinateServerOperator;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.ElectServerOperator;
import com.anur.core.util.HanabiExecutors;
import com.anur.io.store.common.Operation;
import com.anur.io.store.common.OperationTypeEnum;
import com.anur.io.store.manager.LogManager;

/**
 * Created by Anur IjuoKaruKas on 2019/3/13
 */
public class Bootstrap {

    private static boolean RUNNING = true;

    public static void main(String[] args) throws InterruptedException {

        HanabiExecutors.submit(() -> {
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

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (int i = 0; i < 1000; i++) {

                logManager.append(new Operation(OperationTypeEnum.SETNX, "k", "v"));
            }
        });

        while (RUNNING) {
            Thread.sleep(1000);
        }
    }
}
