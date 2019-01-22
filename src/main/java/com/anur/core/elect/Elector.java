package com.anur.core.elect;

import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.elect.vote.base.VoteController;
import com.anur.core.elect.vote.base.VotesRecord;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.lock.ReentrantLocker;
import com.anur.timewheel.Timer;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 *
 * 进行leader选举的核心控制类
 */
public class Elector extends ReentrantLocker implements VoteController {

    /**
     * 世代信息
     */
    private int generation;

    private Voter voter;

    private VotesRecord votesRecord;

    private Timer timer;

    public Elector() {
        this.generation = 0;
        this.voter = new Voter();
        this.votesRecord = new VotesRecord();
        this.timer = Timer.getInstance();
    }

    /**
     * 开始进行选举
     */
    public void beginElect() {
        this.lockSupplier(() -> {
            updateGeneration();

            Votes votes = new Votes(generation, InetSocketAddressConfigHelper.getServerName(), true);
            // 给自己投票
            voter.vote(votes);

            // 让其他服务给自己投一票
            voter.askForVote();
            return null;
        });
    }

    /**
     * 强制更新世代信息
     */
    public void updateGeneration() {
        this.lockSupplier(() -> {
            generation = generation++;
            if (!votesRecord.initVotesRecord(generation) || !voter.initVotesBox(generation)) {
                updateGeneration();
            }
            return null;
        });
    }
}
