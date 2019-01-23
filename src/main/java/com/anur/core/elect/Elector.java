package com.anur.core.elect;

import com.anur.core.elect.vote.base.VoteController;
import com.anur.core.lock.ReentrantLocker;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 *
 * 进行leader选举的核心控制类
 */
public class Elector extends ReentrantLocker implements VoteController {

}
