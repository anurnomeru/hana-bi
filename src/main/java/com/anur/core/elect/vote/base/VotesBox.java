package com.anur.core.elect.vote.base;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiCluster;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.lock.ReentrantLocker;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 *
 * 自己的投票箱
 */
public abstract class VotesBox extends ReentrantLocker {

    /**
     * 投票箱
     */
    protected Set<String/* serverName */> box;

    /**
     * 投票给了谁的投票记录
     */
    protected Votes voteRecord;

    /**
     * 该投票箱的世代信息
     */
    protected int generation;

    public VotesBox() {
        this.generation = 0;
        this.box = new HashSet<>();
    }

    /**
     * 当选票大于一半以上时调用这个方法，如何去成为一个leader
     */
    protected abstract void becomeLeader(List<HanabiCluster> hanabiClusterList);

    /**
     * 如何向其他节点发起拉票请求
     */
    protected abstract void askForVote(List<HanabiCluster> hanabiClusterList);

    /**
     * 强制更新世代信息
     */
    public void updateGeneration() {
        this.lockSupplier(() -> {
            generation = generation++;
            if (!this.initVotesBox(generation)) {
                updateGeneration();
            }
            return null;
        });
    }

    /**
     * 开始进行选举
     *
     * 1、首先更新一下世代信息，重置投票箱和投票记录
     * 2、首先给自己投一票
     * 3、请求其他服务，要求其他服务给自己投票（需要子类去实现）
     */
    public void beginElect() {
        this.lockSupplier(() -> {
            updateGeneration();

            Votes votes = new Votes(generation, InetSocketAddressConfigHelper.getServerName());
            // 给自己投票箱投票
            this.receiveVotes(votes);
            // 记录一下，自己给自己投了票
            this.voteRecord = votes;

            // 让其他服务给自己投一票
            this.askForVote(InetSocketAddressConfigHelper.getCluster());
            return null;
        });
    }

    /**
     * 给当前服务的投票箱投票
     */
    public void receiveVotes(Votes votes) {
        this.lockSupplier(() -> {
            if (votes.getGeneration() > this.generation) {// 如果有选票的世代已经大于当前世代，那么重置投票箱
                this.initVotesBox(this.generation);
            } else if (this.generation > votes.getGeneration()) {// 如果选票的世代小于当前世代，投票无效
                return null;
            }

            box.add(votes.getServerName());

            List<HanabiCluster> hanabiClusterList = InetSocketAddressConfigHelper.getCluster();
            int clusterSize = hanabiClusterList.size();
            int votesNeed = clusterSize / 2 + 1;

            // 如果获得的选票已经大于了集群数量的一半以上，则成为leader
            if (box.size() >= votesNeed) {
                this.becomeLeader(hanabiClusterList);
            }

            return null;
        });
    }

    /**
     * 某个服务来请求投票了，只有当世代大于当前世代，才有投票一说，其他情况都是失败的
     *
     * 返回结果
     *
     * 为true代表接受投票成功。
     * 为false代表已经给其他服务投过票了，
     */
    public boolean vote(Votes votes) {
        return this.lockSupplier(() -> {
            if (votes.getGeneration() > this.generation) {
                this.initVotesBox(votes.getGeneration());
                this.voteRecord = votes;
            }
            return votes.equals(this.voteRecord);
        });
    }

    /**
     * 初始化投票箱
     */
    private boolean initVotesBox(int generation) {
        return this.lockSupplier(() -> {
            if (generation > this.generation) {// 如果有选票的世代已经大于当前世代，那么重置投票箱
                this.generation = generation;
                this.voteRecord = null;
                this.box = new HashSet<>();
                return true;
            }
            return false;
        });
    }

    private static class UnbelievableException extends HanabiException {

        public UnbelievableException(String message) {
            super(message);
        }
    }
}
