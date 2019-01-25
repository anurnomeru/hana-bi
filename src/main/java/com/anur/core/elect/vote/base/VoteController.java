package com.anur.core.elect.vote.base;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiCluster;
import com.anur.core.elect.vote.model.Canvass;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.lock.ReentrantLocker;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 *
 * 投票控制器
 */
public abstract class VoteController extends ReentrantLocker {

    private Logger logger;

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

    public VoteController() {
        this.generation = 0;
        this.box = new HashSet<>();
        logger = LoggerFactory.getLogger(VoteController.class);
        logger.info("初始化 投票控制器 VoteController");
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
            logger.info("强制更新当前世代 =====> " + generation);
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
            logger.info("本服务开始发起选举 =====> ");
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
            logger.info("收到来自服务 {} 的选票，其世代为 {}", votes.getServerName(), votes.getGeneration());

            if (votes.getGeneration() > this.generation) {// 如果有选票的世代已经大于当前世代，那么重置投票箱
                logger.info("来自服务 {} 的选票世代大于当前世代", votes.getServerName());
                throw new UnbelievableException("出现了不可能出现的情况！选票大于了当前的世代");
            } else if (this.generation > votes.getGeneration()) {// 如果选票的世代小于当前世代，投票无效
                logger.info("来自服务 {} 的选票世代小于当前世代，选票无效", votes.getServerName());
                return null;
            }

            logger.info("来自服务 {} 的选票有效，投票箱 + 1", votes.getServerName());
            box.add(votes.getServerName());

            List<HanabiCluster> hanabiClusterList = InetSocketAddressConfigHelper.getCluster();
            int clusterSize = hanabiClusterList.size();
            int votesNeed = clusterSize / 2 + 1;
            logger.info("本服务当前投票箱进度 {}/{}", box.size(), votesNeed);

            // 如果获得的选票已经大于了集群数量的一半以上，则成为leader
            if (box.size() >= votesNeed) {
                logger.info("====================== 选票过半 ====================== ，准备上位成为 leader", votes.getServerName());
                this.becomeLeader(hanabiClusterList);
            }

            return null;
        });
    }

    /**
     * 某个服务来拉票了，只有当世代大于当前世代，才有投票一说，其他情况都是失败的
     *
     * 返回结果
     *
     * 为true代表接受投票成功。
     * 为false代表已经给其他服务投过票了，
     */
    public Canvass vote(Votes votes) {
        return this.lockSupplier(() -> {
            logger.info("收到来自服务 {} 的拉票请求，其世代为 {}", votes.getServerName(), votes.getGeneration());
            if (votes.getGeneration() > this.generation) {

                this.initVotesBox(votes.getGeneration());
                this.voteRecord = votes;
            }
            return new Canvass(this.generation, votes.equals(this.voteRecord));
        });
    }

    /**
     * 初始化投票箱
     */
    private boolean initVotesBox(int generation) {
        return this.lockSupplier(() -> {
            if (generation > this.generation) {// 如果有选票的世代已经大于当前世代，那么重置投票箱
                logger.info("更新世代：旧世代 {} => 新世代 {}", this.generation, generation);
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
