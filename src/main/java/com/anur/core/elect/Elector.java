package com.anur.core.elect;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 *
 * 进行leader选举的核心控制类
 */
public abstract class Elector {

    /**
     * 世代信息
     */
    private AtomicInteger genneration;
}
