package com.anur.core;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 *
 * 进行leader选举的核心控制类
 */
public class Elector {

    public static void main(String[] args) {
        ElectServerOperator.getInstance()
                           .start();
        VoteOperator.getInstance()
                    .beginElect();
    }
}
