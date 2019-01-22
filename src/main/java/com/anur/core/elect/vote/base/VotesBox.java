package com.anur.core.elect.vote.base;

import java.util.HashMap;
import java.util.Map;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.lock.ReentrantLocker;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 *
 * 自己的投票箱
 */
public abstract class VotesBox extends ReentrantLocker {

    /**
     * 如何去成为一个leader
     */
    protected abstract void becomeLeader();

    /**
     * 投票箱
     */
    protected Map<String/* serverName */, Boolean/* active */> box;

    /**
     * 该投票箱的世代信息
     */
    protected int generation;

    public VotesBox() {
        this.generation = 0;
        this.box = new HashMap<>();
    }

    /**
     * 当选票大于一半以上时调用这个方法
     */
    protected abstract void doWhenMoreThenHalfOfVotes();

    /**
     * 初始化投票箱
     */
    public boolean initVotesBox(int generation) {
        return this.lockSupplier(() -> {
            if (generation > this.generation) {// 如果有选票的世代已经大于当前世代，那么重置投票箱
                this.generation = generation;
                box = new HashMap<>();
                return true;
            }
            return false;
        });
    }

    /**
     * 给投票箱投票
     */
    public int vote(Votes votes) {
        return this.lockSupplier(() -> {
            if (votes.getGeneration() > this.generation) {// 如果有选票的世代已经大于当前世代，那么重置投票箱
                this.initVotesBox(this.generation);// 必定返回true
            }

            box.put(votes.getServerName(), votes.isActive());

            int clusterSize = InetSocketAddressConfigHelper.getCluster()
                                                           .size();
            int votesNeed = clusterSize / 2 + 1;

            // 如果获得的选票已经大于了集群数量的一半以上，则成为leader
            if (box.entrySet()
                   .size() >= votesNeed) {
                this.doWhenMoreThenHalfOfVotes();
            }

            return this.generation;
        });
    }
}
