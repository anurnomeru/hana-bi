package com.anur.core.elect.vote;

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
}
