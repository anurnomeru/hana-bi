package com.anur.core.elect;

import java.util.List;
import com.anur.config.InetSocketAddressConfigHelper.HanabiCluster;
import com.anur.core.elect.vote.base.VoteController;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 */
public class Voter extends VoteController {

    @Override
    protected void becomeLeader(List<HanabiCluster> hanabiClusterList) {

    }

    @Override
    protected void askForVote(List<HanabiCluster> hanabiClusterList) {

    }
}
