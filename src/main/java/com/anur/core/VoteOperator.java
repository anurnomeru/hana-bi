package com.anur.core;

import java.util.List;
import com.anur.config.InetSocketAddressConfigHelper.HanabiCluster;
import com.anur.core.elect.vote.base.VoteController;

/**
 * Created by Anur IjuoKaruKas on 2/1/2019
 *
 * 投票操作类
 */
public class VoteOperator extends VoteController {

    private volatile static VoteOperator INSTANCE;

    public static VoteOperator getInstance() {
        if (INSTANCE == null) {
            synchronized (VoteController.class) {
                if (INSTANCE == null) {
                    INSTANCE = new VoteOperator();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void askForVote(List<HanabiCluster> hanabiClusterList) {

    }

    @Override
    protected void becomeLeader(List<HanabiCluster> hanabiClusterList) {

    }
}
