package com.anur.core.elect;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.elect.constant.TaskEnum;
import com.anur.core.elect.model.Canvass;
import com.anur.core.elect.model.Votes;
import com.anur.core.lock.ReentrantLocker;
import com.anur.exception.HanabiException;
import com.anur.timewheel.TimedTask;
import io.netty.util.internal.StringUtil;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 *
 * 投票控制器
 */
public class VoteOperator extends ReentrantLocker {

    private volatile static VoteOperator INSTANCE;

    public static VoteOperator getInstance() {
        if (INSTANCE == null) {
            synchronized (VoteOperator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new VoteOperator();
                }
            }
        }
        return INSTANCE;
    }

    private Logger logger = LoggerFactory.getLogger(VoteOperator.class);

    /**
     * 所有正在跑的任务
     */
    private Map<TaskEnum, TimedTask> taskMap;

    /**
     * 投票箱
     */
    private Set<String/* serverName */> box;

    /**
     * 投票给了谁的投票记录
     */
    private Votes voteRecord;

    /**
     * 该投票箱的世代信息
     */
    private int generation;

    /**
     * 缓存一份集群信息，因为集群信息是可能变化的，我们要保证在一次选举中，集群信息是不变的
     */
    private List<HanabiNode> clusters;

    private VoteOperator() {
        this.generation = 0;
        this.taskMap = new HashMap<>();
        this.box = new HashSet<>();
        logger.info("初始化 投票控制器 VoteController");
    }

    /**
     * 当选票大于一半以上时调用这个方法，如何去成为一个leader
     */
    private void becomeLeader() {
        // TODO 取消定时任务
    }

    /**
     * 真正的执行方法，并提供续约功能
     */
    private void askForVote(Votes votes, long delayMs) {
        this.lockSupplier(() -> {
            TimedTask timedTask = new TimedTask(delayMs, () -> {

                this.clusters.forEach(
                    hanabiNode -> {
                        if (!this.box.contains(hanabiNode.getServerName())) {
                            ElectClientOperatorFacade.askForVote(hanabiNode, generation, votes);
                        }
                    });

                // 拉票续约
                this.askForVote(votes, 200);
            });
            taskMap.put(TaskEnum.ASK_FOR_VOTES, timedTask);
            return null;
        });
    }

    /**
     * 强制更新世代信息
     */
    private void updateGeneration() {
        this.lockSupplier(() -> {

            logger.info("强制更新当前世代 =====> " + this.generation);

            this.clusters = InetSocketAddressConfigHelper.getCluster();
            logger.info("更新节点信息     =====> " + this.generation);

            if (!this.initVotesBox(this.generation + 1)) {
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
     * 3、请求其他节点，要求其他节点给自己投票（需要子类去实现）
     */
    public void beginElect() {
        this.lockSupplier(() -> {
            logger.info("本节点开始发起选举 =====> ");
            updateGeneration();

            Votes votes = new Votes(generation, InetSocketAddressConfigHelper.getServerName());
            // 给自己投票箱投票
            this.receiveVotes(votes);
            // 记录一下，自己给自己投了票
            this.voteRecord = votes;

            // 让其他节点给自己投一票
            this.askForVote(new Votes(this.generation, InetSocketAddressConfigHelper.getServerName()), 0);
            return null;
        });
    }

    /**
     * 给当前节点的投票箱投票
     */
    private void receiveVotes(Votes votes) {
        this.lockSupplier(() -> {
            logger.info("收到来自节点 {} 的选票，其世代为 {}", votes.getServerName(), votes.getGeneration());

            if (votes.getGeneration() > this.generation) {// 如果有选票的世代已经大于当前世代，那么重置投票箱
                logger.info("来自节点 {} 的选票世代大于当前世代", votes.getServerName());
                throw new UnbelievableException("出现了不可能出现的情况！选票大于了当前的世代");
            } else if (this.generation > votes.getGeneration()) {// 如果选票的世代小于当前世代，投票无效
                logger.info("来自节点 {} 的选票世代小于当前世代，选票无效", votes.getServerName());
                return null;
            }

            logger.info("来自节点 {} 的选票有效，投票箱 + 1", votes.getServerName());
            box.add(votes.getServerName());

            List<HanabiNode> hanabiNodeList = this.clusters;
            int clusterSize = hanabiNodeList.size();
            int votesNeed = clusterSize / 2 + 1;
            logger.info("集群中共 {} 个节点，本节点当前投票箱进度 {}/{}", hanabiNodeList.size(), box.size(), votesNeed);

            // 如果获得的选票已经大于了集群数量的一半以上，则成为leader
            if (box.size() >= votesNeed) {
                logger.info("====================== 选票过半 ====================== ，准备上位成为 leader", votes.getServerName());
                this.becomeLeader();
            }

            return null;
        });
    }

    /**
     * 某个节点来拉票了，只有当世代大于当前世代，才有投票一说，其他情况都是失败的
     *
     * 返回结果
     *
     * 为true代表接受投票成功。
     * 为false代表已经给其他节点投过票了，
     */
    public Canvass vote(Votes votes) {
        return this.lockSupplier(() -> {
            logger.info("收到节点 {} 的拉票请求，其世代为 {}", votes.getServerName(), votes.getGeneration());

            String cause = "";

            if (votes.getGeneration() > this.generation) {

                this.initVotesBox(votes.getGeneration());
                this.voteRecord = votes;
            } else {
                cause = "选票世代小于当前世代";
            }

            boolean result = votes.equals(this.voteRecord);

            if (result) {
                logger.info("投票记录更新成功，在世代 {}，本节点投票给 => {} 节点", this.generation, this.voteRecord.getServerName());
            } else {
                cause = Optional.of(cause)
                                .filter(s -> !StringUtil.isNullOrEmpty(s))
                                .map(s -> s = String.format("在世代 %s，本节点已投票给 => %s 节点", this.generation, this.voteRecord.getServerName()))
                                .orElse(cause);
                logger.info("投票记录更新失败，原因：{}", cause);
            }

            return new Canvass(this.generation, result);
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
                logger.info("清空本节点投票箱，清空本节点投票记录", this.generation, generation);

                // 先取消之前的拉票任务
                Optional.ofNullable(taskMap.get(TaskEnum.ASK_FOR_VOTES))
                        .ifPresent(TimedTask::cancel);
                logger.info("取消本节点在上个世代的拉票任务");
                return true;
            }
            return false;
        });
    }

    /**
     * 不要乱用这个来做本地的判断，因为它并不可靠！！！
     */
    public int getGeneration() {
        return generation;
    }

    private static class UnbelievableException extends HanabiException {

        public UnbelievableException(String message) {
            super(message);
        }
    }
}
