package com.anur.core.elect.vote.base;

import com.anur.core.elect.vote.model.Votes;
import com.anur.core.lock.ReentrantLocker;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 *
 * 自己的投票记录
 */
public class VotesRecord extends ReentrantLocker {

    /**
     * 该投票箱的世代信息
     */
    private int generation;

    /**
     * 投票给了谁
     */
    private Votes votes;

    public VotesRecord() {
        this.generation = 0;
    }

    /**
     * 重置投票记录
     */
    public boolean initVotesRecord(int generation) {
        return this.lockSupplier(() -> {
            if (generation > this.generation) {// 如果有选票的世代已经大于当前世代，那么重置投票记录
                this.generation = generation;
                this.votes = null;
                return true;
            }
            return false;
        });
    }

    /**
     * 给某个服务投票，返回最新的选票
     */
    public Votes vote(Votes votes) {
        return this.lockSupplier(() -> {
            if (votes.getGeneration() > this.generation) {
                this.initVotesRecord(votes.getGeneration());
                this.votes = votes;
            }
            return this.votes;
        });
    }
}
