package com.anur.core.elect;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.coder.Coder;
import com.anur.core.coder.ProtocolEnum;
import com.anur.core.elect.constant.NodeRole;
import com.anur.core.elect.constant.TaskEnum;
import com.anur.core.elect.model.Canvass;
import com.anur.core.elect.model.Votes;
import com.anur.core.lock.ReentrantLocker;
import com.anur.core.util.ChannelManager;
import com.anur.core.util.ChannelManager.ChannelType;
import com.anur.core.util.HanabiExecutors;
import com.anur.exception.HanabiException;
import com.anur.timewheel.TimedTask;
import com.anur.timewheel.Timer;
import io.netty.util.internal.StringUtil;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 *
 * 投票控制器
 */
public class ElectOperator extends ReentrantLocker implements Runnable {

    private volatile static ElectOperator INSTANCE;

    private static Random RANDOM = new Random();

    public static ElectOperator getInstance() {
        if (INSTANCE == null) {
            synchronized (ElectOperator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ElectOperator();
                    HanabiExecutors.submit(INSTANCE);
                }
            }
        }
        return INSTANCE;
    }

    private CountDownLatch startLatch;

    private Logger logger = LoggerFactory.getLogger(ElectOperator.class);

    /**
     * 当前节点的角色
     */
    private NodeRole nodeRole;

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
    private long generation;

    /**
     * 缓存一份集群信息，因为集群信息是可能变化的，我们要保证在一次选举中，集群信息是不变的
     */
    private List<HanabiNode> clusters;

    private ElectOperator() {
        this.generation = -1;
        this.taskMap = new ConcurrentHashMap<>();
        this.box = new HashSet<>();
        this.nodeRole = NodeRole.Follower;
        this.startLatch = new CountDownLatch(1);
        logger.info("初始化选举控制器 ElectOperator");
    }

    /**
     * 强制更新世代信息
     */
    private void updateGeneration(String reason) {
        this.lockSupplier(() -> {
            logger.info("强制更新当前世代 {} -> {}", this.generation, this.generation + 1);

            this.clusters = InetSocketAddressConfigHelper.getCluster();
            logger.info("更新集群节点信息     =====> " + JSON.toJSONString(this.clusters));

            if (!this.initVotesBox(this.generation + 1, reason)) {
                updateGeneration(reason);
            }
            return null;
        });
    }

    /**
     * 开始进行选举
     *
     * 1、首先更新一下世代信息，重置投票箱和投票记录
     * 2、成为候选者
     * 3、给自己投一票
     * 4、请求其他节点，要求其他节点给自己投票
     */
    private void beginElect() {
        this.lockSupplier(() -> {
            logger.info("本节点开始发起选举 ========================================> ");
            updateGeneration("本节点发起了选举");

            // 成为候选者
            if (this.becomeCandidate()) {
                Votes votes = new Votes(generation, InetSocketAddressConfigHelper.getServerName());

                // 给自己投票箱投票
                this.receiveVotes(votes);

                // 记录一下，自己给自己投了票
                this.voteRecord = votes;

                // 让其他节点给自己投一票
                this.askForVoteTask(new Votes(this.generation, InetSocketAddressConfigHelper.getServerName()), 0);
            }
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
    public Canvass receiveCanvass(Votes votes) {
        return this.lockSupplier(() -> {
            logger.info("收到节点 {} 的拉票请求，其世代为 {}", votes.getServerName(), votes.getGeneration());

            String cause = "";

            if (votes.getGeneration() >= this.generation && this.voteRecord == null) {
                this.voteRecord = votes;
            } else {
                cause = String.format("选票 %s 世代小于当前世代 %s", votes.getGeneration(), this.generation);
            }

            boolean result = votes.equals(this.voteRecord);

            if (result) {
                logger.info("投票记录更新成功，在世代 {}，本节点投票给 => {} 节点", this.generation, this.voteRecord.getServerName());
            } else {
                try {
                    cause = Optional.of(cause)
                                    .filter(StringUtil::isNullOrEmpty)
                                    .map(s -> String.format("在世代 %s，本节点已投票给 => %s 节点", this.generation, this.voteRecord.getServerName()))
                                    .orElse(cause);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println();
                }
                logger.info("投票记录更新失败，原因：{}", cause);
            }

            return new Canvass(this.generation, result);
        });
    }

    /**
     * 当选票大于一半以上时调用这个方法，如何去成为一个leader
     */
    private void becomeLeader() {
        this.lockSupplier(() -> {
            logger.info("本节点角色由 {} 变更为 {}", this.nodeRole.name(), NodeRole.Leader.name());
            this.nodeRole = NodeRole.Leader;
            this.cancelAllTask();
            return null;
        });
    }

    /**
     * 初始化投票箱
     *
     * 1、成为follower
     * 2、先取消所有的定时任务
     * 3、重置本地变量
     * 4、新增成为Candidate的定时任务
     */
    private boolean initVotesBox(long generation, String reason) {
        return this.lockSupplier(() -> {
            if (generation > this.generation) {// 如果有选票的世代已经大于当前世代，那么重置投票箱
                logger.info("初始化投票箱，原因：{}", reason);

                // 1、成为follower
                this.becomeFollower();

                // 2、先取消所有的定时任务
                this.cancelAllTask();

                // 3、重置本地变量
                logger.info("更新世代：旧世代 {} => 新世代 {}", this.generation, generation);
                this.generation = generation;
                this.voteRecord = null;
                this.box = new HashSet<>();

                // 4、新增成为Candidate的定时任务
                this.becomeCandidateTask();
                return true;
            } else {
                return false;
            }
        });
    }

    /**
     * 成为候选者的任务，（重复调用则会取消之前的任务，收到来自leader的心跳包，就可以重置一下这个任务）
     *
     * 没加锁，因为这个任务需要频繁被调用，只要收到leader来的消息就可以调用一下
     */
    private void becomeCandidateTask() {
        this.cancelCandidateTask();

        // The election timeout is randomized to be between 150ms and 300ms.
        long electionTimeout = 150 + (int) (150 * RANDOM.nextFloat());
        TimedTask timedTask = new TimedTask(electionTimeout, this::beginElect);
        Timer.getInstance()
             .addTask(timedTask);

        logger.info("本节点将于 {} ms 后转变为候选者", electionTimeout);
        taskMap.put(TaskEnum.BECOME_CANDIDATE, timedTask);
    }

    /**
     * 取消成为候选者的任务，成为leader，或者调用 {@link #becomeCandidateTask} （心跳）
     * 时调用，也就是说如果没能成为leader，又会重新进行一次选主，直到成为leader，或者follower。
     */
    private void cancelCandidateTask() {
        this.lockSupplier(() -> {
            Optional.ofNullable(taskMap.get(TaskEnum.BECOME_CANDIDATE))
                    .ifPresent(timedTask -> {
                        logger.info("取消成为候选者的定时任务");
                        timedTask.cancel();
                    });
            return null;
        });
    }

    private void cancelAllTask() {
        this.lockSupplier(() -> {
            logger.info("取消本节点在上个世代的所有定时任务");
            taskMap.values()
                   .forEach(TimedTask::cancel);
            return null;
        });
    }

    /**
     * 成为候选者
     */
    private boolean becomeCandidate() {
        return this.lockSupplier(() -> {
            if (this.nodeRole != NodeRole.Leader) {

                if (this.nodeRole == NodeRole.Follower) {
                    logger.info("本节点角色由 {} 变更为 {}", this.nodeRole.name(), NodeRole.Candidate.name());
                    this.nodeRole = NodeRole.Candidate;
                }
                return true;
            } else {
                logger.info("本节点的角色已经是 {} ，无法变更为 {}", this.nodeRole.name(), NodeRole.Candidate.name());
                return false;
            }
        });
    }

    private void becomeFollower() {
        this.lockSupplier(() -> {
            logger.info("本节点角色由 {} 变更为 {}", this.nodeRole.name(), NodeRole.Follower.name());
            this.nodeRole = NodeRole.Follower;
            return null;
        });
    }

    /**
     * 拉票请求的任务
     */
    private void askForVoteTask(Votes votes, long delayMs) {
        this.lockSupplier(() -> {
            TimedTask timedTask = new TimedTask(delayMs, () -> {

                this.clusters.forEach(
                    hanabiNode -> {
                        // 如果还没收到这个节点的选票，就继续发
                        if (!this.box.contains(hanabiNode.getServerName())) {
                            // 确保和其他选举服务器保持连接
                            ElectClientOperator.getInstance(hanabiNode)
                                               .start();

                            // 向其他节点发送拉票请求
                            Optional.ofNullable(ChannelManager.getInstance(ChannelType.ELECT)
                                                              .getChannel(hanabiNode.getServerName()))
                                    .ifPresent(channel -> {
                                        logger.info("正向节点 {} [{}:{}] 发送世代 {} 的拉票请求...", hanabiNode.getServerName(), hanabiNode.getHost(), hanabiNode.getElectionPort(), this.generation);
                                        channel.writeAndFlush(Coder.encodeToByteBuf(ProtocolEnum.CANVASSED, votes));
                                    });
                        }
                    });

                // 拉票续约（如果没有得到其他节点的回应，就继续发 voteTask）
                this.askForVoteTask(votes, 70);
            });
            Timer.getInstance()
                 .addTask(timedTask);
            taskMap.put(TaskEnum.ASK_FOR_VOTES, timedTask);
            return null;
        });
    }

    public void updateGenWhileReceiveHigherGen(String serverName, long generation) {
        this.initVotesBox(generation, String.format(String.format("收到了来自节点 %s 的请求，其世代 %s 大于当前世代 %s", serverName, generation, this.getGeneration()), this.getGeneration()));
    }

    /**
     * 不要乱用这个来做本地的判断，因为它并不可靠！！！
     */
    public long getGeneration() {
        return generation;
    }

    public void start() {
        startLatch.countDown();
    }

    @Override
    public void run() {
        this.lockSupplier(() -> {
            logger.info("初始化选举控制器 待启动");
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("初始化选举控制器 启动中");
            this.becomeCandidateTask();
            return null;
        });
    }

    private static class UnbelievableException extends HanabiException {

        public UnbelievableException(String message) {
            super(message);
        }
    }
}
