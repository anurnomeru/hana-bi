package com.anur.core.elect;

import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.elect.vote.base.VoteController;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.lock.ReentrantLocker;
import com.anur.exception.HanabiException;
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

    private Timer timer;

    public Elector() {
        this.generation = 0;
        this.voter = new Voter();
        this.timer = Timer.getInstance();
    }

    /**
     * 开始进行选举
     */
    public void beginElect() {
        this.lockSupplier(() -> {
            updateGeneration();

            Votes votes = new Votes(generation, InetSocketAddressConfigHelper.getServerName());
            // 给自己投票
            voter.receiveVotes(votes);

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
            if (!voter.initVotesBox(generation)) {
                updateGeneration();
            }
            return null;
        });
    }

    /**
     * 强制更新世代信息，如果世代比当前大，那么就更新，并返回true
     */
    public boolean updateGeneration(int generation) {
        return this.lockSupplier(() -> {
            if (generation > this.generation) {
                if (!voter.initVotesBox(generation)) {// 不会出现这种情况
                    throw new UnbelievableException("不可能出现这种情况！！");
                }
                return true;
            }
            return false;
        });
    }

    /**
     * todo 收到更大的vote时，需要取消自己的选举
     */
    public boolean voteResoponse(Votes votes) {
        return this.lockSupplier(() -> {
            // 尝试更新一下现在的世代
            updateGeneration(votes.getGeneration());

            // 尝试进行投票，如果投票后，票还是同一张，代表投票成功
            Votes latestVotes = voter.vote(votes);
            return votes.equals(latestVotes);
        });
    }

    public static class UnbelievableException extends HanabiException {

        public UnbelievableException(String message) {
            super(message);
        }
    }
}
