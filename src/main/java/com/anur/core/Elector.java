package com.anur.core;

import com.anur.core.coordinate.CoordinateServerOperator;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.ElectServerOperator;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 *
 * 进行leader选举的核心控制类
 */
public class Elector {

    public static void main(String[] args) {
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
                     .start();
    }
}
