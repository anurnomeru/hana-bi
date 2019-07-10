package com.anur;

import com.anur.core.coordinate.apis.FollowerCoordinateManager;
import com.anur.core.coordinate.operator.CoordinateServerOperator;
import com.anur.core.elect.operator.ElectOperator;
import com.anur.core.elect.operator.ElectServerOperator;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.Operation;
import com.anur.core.util.HanabiExecutors;
import com.anur.io.store.log.LogManager;

/**
 * Created by Anur IjuoKaruKas on 2019/3/13
 */
public class Bootstrap {

    private static boolean RUNNING = true;

    public static void main(String[] args) throws InterruptedException {

        HanabiExecutors.execute(() -> {

            /**
             * 日志一致性控制器
             */
            FollowerCoordinateManager forInitial = FollowerCoordinateManager.INSTANCE;

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
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                System.out.println("开始append");
                for (int i = 0; i < 100000; i++) {
                    Operation operation = new Operation(OperationTypeEnum.SETNX, "setAnur", "ToIjuoKaruKas");
                    LogManager.getINSTANCE()
                              .append(operation);
                }
            } catch (Exception e) {
            }
        });

        while (RUNNING) {
            Thread.sleep(1000);
        }
    }
}
