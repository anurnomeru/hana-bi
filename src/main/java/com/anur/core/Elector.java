package com.anur.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.ElectServerOperator;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 *
 * 进行leader选举的核心控制类
 */
public class Elector {

    public static void main(String[] args) {
        ElectServerOperator.getInstance()
                           .start();
        ElectOperator.getInstance()
                     .start();
    }
}
