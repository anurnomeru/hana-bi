package com.anur.core.elect.vote;

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
    protected int generation;

    /**
     * 投票给了谁
     */
    public String votedServerName;

    public VotesRecord() {
        this.generation = 0;
    }

    /**
     * 重置投票记录
     */
    public boolean initVotesRecord(int generation) {
        return this.lockSupplier(() -> {
            if (generation > this.generation) {
                this.generation = generation;
                this.votedServerName = null;
                return true;
            }
            return false;
        });
    }

    /**
     * 给某个服务投票，投票成功则返回true
     * 1、新的一轮投票（generation更大），返回true
     * 2、当前一轮的投票，如果投的是同一个server，返回true
     */
    public boolean vote(Votes votes) {
        return this.lockSupplier(() -> {
            if (votes.getGeneration() > this.generation) {
                this.initVotesRecord(votes.getGeneration());
                this.votedServerName = votes.getServerName();
                return true;
            } else if (votes.getGeneration() == this.generation) {
                if (votes.getServerName()
                         .equals(votedServerName)) {
                    return true;
                }
            }
            return false;
        });
    }
}
