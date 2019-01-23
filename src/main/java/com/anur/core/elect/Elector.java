package com.anur.core.elect;

import java.util.List;
import com.anur.config.InetSocketAddressConfigHelper.HanabiCluster;
import com.anur.core.elect.vote.base.VoteController;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 *
 * 进行leader选举的核心控制类
 */
public class Elector extends VoteController {

    @Override
    protected void becomeLeader(List<HanabiCluster> hanabiClusterList) {

    }

    @Override
    protected void askForVote(List<HanabiCluster> hanabiClusterList) {

    }
}
