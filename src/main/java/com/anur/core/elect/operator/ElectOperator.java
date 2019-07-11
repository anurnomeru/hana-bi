package com.anur.core.elect.operator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.anur.config.ElectConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.elect.ElectMeta;
import com.anur.core.elect.constant.NodeRole;
import com.anur.core.elect.constant.TaskEnum;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.elect.model.Votes;
import com.anur.core.elect.model.VotesResponse;
import com.anur.core.lock.ReentrantLocker;
import com.anur.core.util.ChannelManager;
import com.anur.core.util.ChannelManager.ChannelType;
import com.anur.core.util.TimeUtil;
import com.anur.exception.ElectException;
import com.anur.io.core.coder.ElectCoder;
import com.anur.io.core.coder.ElectProtocolEnum;
import com.anur.timewheel.TimedTask;
import com.anur.timewheel.Timer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 *
 * 投票控制器
 */
public class ElectOperator extends ReentrantLocker implements Runnable {

    private static final long ELECTION_TIMEOUT_MS = ElectConfigHelper.getElectionTimeoutMs();

    private static final long VOTES_BACK_OFF_MS = ElectConfigHelper.getVotesBackOffMs();

    private static final long HEART_BEAT_MS = ElectConfigHelper.getHeartBeatMs();

    /**
     * 协调器独享线程
     */
    private static Executor ElectControllerPool = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setPriority(10)
                                                                                                            .setNameFormat("Controller")
                                                                                                            .build());

    private volatile static ElectOperator INSTANCE;

    private static Random RANDOM = new Random();

    public static ElectOperator getInstance() {
        if (INSTANCE == null) {
            synchronized (ElectOperator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ElectOperator();
                    ElectControllerPool.execute(INSTANCE);
                }
            }
        }

        return INSTANCE;
    }

    private Logger logger = LoggerFactory.getLogger(ElectOperator.class);

    private CountDownLatch startLatch = new CountDownLatch(2);

    private ElectMeta meta = ElectMeta.INSTANCE;

    /**
     * 所有正在跑的定时任务
     */
    Map<TaskEnum, TimedTask> taskMap = new ConcurrentHashMap<>();

    /**
     * 强制更新世代信息
     */
    private void updateGeneration(String reason) {
        this.lockSupplier(() -> {
            logger.debug("强制更新当前世代 {} -> {}", meta.getGeneration(), meta.getGeneration() + 1);

            if (!this.init(meta.getGeneration() + 1, reason)) {
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
    private void beginElect(long generation) {
        this.lockSupplier(() -> {

            if (meta.getGeneration() != generation) {// 存在这么一种情况，虽然取消了选举任务，但是选举任务还是被执行了，所以这里要多做一重处理，避免上个周期的任务被执行
                return null;
            }

            if (meta.getBeginElectTime() == 0) {
                meta.setBeginElectTime(TimeUtil.getTime());
            }

            logger.info("Election Timeout 到期，可能期间内未收到来自 Leader 的心跳包或上一轮选举没有在期间内选出 Leader，故本节点即将发起选举");

            updateGeneration("本节点发起了选举");// meta.getGeneration() ++

            // 成为候选者
            logger.info("本节点正式开始世代 {} 的选举", meta.getGeneration());
            if (this.becomeCandidate()) {
                VotesResponse votes = new VotesResponse(meta.getGeneration(), InetSocketAddressConfigHelper.getServerName(), true, false, meta.getGeneration());

                // 给自己投票箱投票
                this.receiveVotesResponse(votes);

                // 记录一下，自己给自己投了票
                meta.setVoteRecord(votes);

                // 让其他节点给自己投一票
                this.askForVoteTask(new Votes(meta.getGeneration(), InetSocketAddressConfigHelper.getServerName()), 0);
            }
            return null;
        });
    }

    /**
     * 某个节点来请求本节点给他投票了，只有当世代大于当前世代，才有投票一说，其他情况都是失败的
     *
     * 返回结果
     *
     * 为true代表接受投票成功。
     * 为false代表已经给其他节点投过票了，
     */
    public VotesResponse receiveVotes(Votes votes) {
        return this.lockSupplier(() -> {
            logger.debug("收到节点 {} 的投票请求，其世代为 {}", votes.getServerName(), votes.getGeneration());
            String cause = "";

            if (votes.getGeneration() < meta.getGeneration()) {
                cause = String.format("投票请求 %s 世代小于当前世代 %s", votes.getGeneration(), meta.getGeneration());
            } else if (meta.getVoteRecord() != null) {
                cause = String.format("在世代 %s，本节点已投票给 => %s 节点", meta.getGeneration(), meta.getVoteRecord()
                                                                                           .getServerName());
            } else {
                meta.setVoteRecord(votes); // 代表投票成功了
            }

            boolean result = votes.equals(meta.getVoteRecord());

            if (result) {
                logger.debug("投票记录更新成功：在世代 {}，本节点投票给 => {} 节点", meta.getGeneration(), meta.getVoteRecord()
                                                                                          .getServerName());
            } else {
                logger.debug("投票记录更新失败：原因：{}", cause);
            }

            String serverName = InetSocketAddressConfigHelper.getServerName();
            return new VotesResponse(meta.getGeneration(), serverName, result, serverName.equals(meta.getLeader()), votes.getGeneration());
        });
    }

    /**
     * 给当前节点的投票箱投票
     */
    public void receiveVotesResponse(VotesResponse votesResponse) {
        this.lockSupplier(() -> {

            // 已经有过回包了，无需再处理
            if (meta.getBox()
                    .containsKey(votesResponse.getServerName())) {
                return null;
            }

            boolean voteSelf = votesResponse.getServerName()
                                            .equals(InetSocketAddressConfigHelper.getServerName());
            if (voteSelf) {
                logger.info("本节点在世代 {} 转变为候选者，给自己先投一票", meta.getGeneration());
            } else {
                logger.info("收到来自节点 {} 的投票应答，其世代为 {}", votesResponse.getServerName(), votesResponse.getGeneration());
            }

            if (votesResponse.isFromLeaderNode()) {
                logger.info("来自节点 {} 的投票应答表明其身份为 Leader，本轮拉票结束。", votesResponse.getServerName());
                this.receiveHeatBeat(votesResponse.getServerName(), votesResponse.getGeneration(),
                    String.format("收到来自 Leader 节点的投票应答，自动将其视为来自 Leader %s 世代 %s 节点的心跳包", meta.getHeartBeat()
                                                                                             .getServerName(), votesResponse.getGeneration()));
            }

            if (meta.getGeneration() > votesResponse.getAskVoteGeneration()) {// 如果选票的世代小于当前世代，投票无效
                logger.info("来自节点 {} 的投票应答世代是以前世代 {} 的选票，选票无效", votesResponse.getServerName(), votesResponse.getAskVoteGeneration());
                return null;
            }

            if (votesResponse.isAgreed()) {
                if (!voteSelf) {
                    logger.info("来自节点 {} 的投票应答有效，投票箱 + 1", votesResponse.getServerName());
                }

                // 记录一下投票结果
                meta.getBox()
                    .put(votesResponse.getServerName(), votesResponse.isAgreed());

                List<HanabiNode> hanabiNodeList = meta.getClusters();
                long voteCount = meta.getBox()
                                     .values()
                                     .stream()
                                     .filter(aBoolean -> aBoolean)
                                     .count();

                logger.info("集群中共 {} 个节点，本节点当前投票箱进度 {}/{}", hanabiNodeList.size(), voteCount, meta.getQuorum());

                // 如果获得的选票已经大于了集群数量的一半以上，则成为leader
                if (voteCount == meta.getQuorum()) {
                    logger.info("选票过半，本节点即将 {} 上位成为 leader 节点", votesResponse.getServerName());
                    this.becomeLeader();
                }
            } else {
                logger.info("节点 {} 在世代 {} 的投票应答为：拒绝给本节点在世代 {} 的选举投票（当前世代 {}）", votesResponse.getServerName(), votesResponse.getGeneration(), votesResponse.getAskVoteGeneration(),
                    meta.getGeneration());

                // 记录一下投票结果
                meta.getBox()
                    .put(votesResponse.getServerName(), votesResponse.isAgreed());
            }

            return null;
        });
    }

    /**
     * 成为候选者
     */
    private boolean becomeCandidate() {
        return this.lockSupplierCompel(() -> {
            meta.becomeCandidate();
            return true;
        });
    }

    /**
     * 当选票大于一半以上时调用这个方法，如何去成为一个leader
     */
    private boolean becomeLeader() {
        return this.lockSupplierCompel(() -> {
            meta.becomeLeader();
            this.cancelAllTask();
            this.initHeartBeatTask();
            return true;
        });
    }

    /**
     * 收到了来自其他节点的 “心跳感染”，如果此心跳感染大于本节点，则将其当做心跳包
     */
    public void receiveHeatBeatInfection(String leaderServerName, long generation, String msg) {
        if ((leaderServerName.equals(meta.getLeader())) && generation == meta.getGeneration()) {
            return;
        } else {
            receiveHeatBeat(leaderServerName, generation, msg);
        }
    }

    public boolean receiveHeatBeat(String leaderServerName, long generation, String msg) {
        return this.lockSupplierCompel(() -> {
            boolean needToSendHeartBeatInfection = true;
            // 世代大于当前世代
            if (generation >= meta.getGeneration()) {
                needToSendHeartBeatInfection = false;
                logger.debug(msg);

                if (meta.getLeader() == null) {

                    logger.info("集群中，节点 {} 已经成功在世代 {} 上位成为 Leader，本节点将成为 Follower，直到与 Leader 的网络通讯出现问题", leaderServerName, generation);

                    Optional.ofNullable(meta.getClusters())
                            .ifPresent(hanabiNodes -> hanabiNodes.forEach(hanabiNode -> {
                                if (!hanabiNode.isLocalNode()) {
                                    Optional.ofNullable(ElectClientOperator.getInstance(hanabiNode))
                                            .filter(electClientOperator -> !electClientOperator.isShutDown())
                                            .ifPresent(ElectClientOperator::ShutDown);
                                }
                            }));
                    // 取消所有任务
                    this.cancelAllTask();

                    // 成为follower
                    meta.becomeFollower();

                    // 将那个节点设为leader节点
                    meta.setLeader(leaderServerName);
                    meta.setBeginElectTime(0L);
                    meta.electionStateChanged(true);
                }

                // 重置成为候选者任务
                this.becomeCandidateAndBeginElectTask(meta.getGeneration());
            }
            return needToSendHeartBeatInfection;
        });
    }

    /**
     * 初始化
     *
     * 1、成为follower
     * 2、先取消所有的定时任务
     * 3、重置本地变量
     * 4、新增成为Candidate的定时任务
     */
    private boolean init(long generation, String reason) {
        return this.lockSupplierCompel(() -> {
            if (generation > meta.getGeneration()) {// 如果有选票的世代已经大于当前世代，那么重置投票箱
                logger.debug("初始化投票箱，原因：{}", reason);

                // 0、更新集群信息
                meta.setClusters(InetSocketAddressConfigHelper.getCluster());
                meta.setQuorum(meta.getClusters()
                                   .size() / 2 + 1);
                logger.debug("更新集群节点信息     ===> " + JSON.toJSONString(meta.getClusters()));

                // 1、成为follower
                meta.becomeFollower();

                // 2、先取消所有的定时任务
                this.cancelAllTask();

                // 3、重置 ElectMeta 变量
                logger.debug("更新世代：旧世代 {} => 新世代 {}", meta.getGeneration(), generation);
                meta.setGeneration(generation);
                meta.setOffset(0L);
                meta.setVoteRecord(null);
                meta.getBox()
                    .clear();
                meta.setLeader(null);
                meta.electionStateChanged(false);

                // 4、新增成为Candidate的定时任务
                this.becomeCandidateAndBeginElectTask(meta.getGeneration());
                return true;
            } else {
                return false;
            }
        });
    }

    /**
     * 拉票请求的任务
     */
    private void askForVoteTask(Votes votes, long delayMs) {
        if (meta.getNodeRole() == NodeRole.Candidate) {
            if (meta.getClusters()
                    .size() == meta.getBox()
                                   .size()) {
                logger.debug("所有的节点都已经应答了本世代 {} 的投票请求，拉票定时任务执行完成", meta.getGeneration());
            }

            this.lockSupplier(() -> {
                if (meta.getNodeRole() == NodeRole.Candidate) {// 只有节点为候选者才可以投票
                    meta.getClusters()
                        .forEach(
                            hanabiNode -> {
                                // 如果还没收到这个节点的选票，就继续发
                                if (!meta.getBox()
                                         .containsKey(hanabiNode.getServerName())) {
                                    // 确保和其他选举服务器保持连接
                                    ElectClientOperator.getInstance(hanabiNode)
                                                       .tryStartWhileDisconnected();

                                    // 向其他节点发送拉票请求
                                    Optional.ofNullable(ChannelManager.getInstance(ChannelType.ELECT)
                                                                      .getChannel(hanabiNode.getServerName()))
                                            .ifPresent(channel -> {
                                                logger.debug("正向节点 {} [{}:{}] 发送世代 {} 的投票请求...", hanabiNode.getServerName(), hanabiNode.getHost(), hanabiNode.getElectionPort(), meta.getGeneration());
                                                channel.writeAndFlush(ElectCoder.encodeToByteBuf(ElectProtocolEnum.VOTES_REQUEST, votes));
                                            });
                                }
                            });

                    TimedTask timedTask = new TimedTask(delayMs, () -> {
                        // 拉票续约（如果没有得到其他节点的回应，就继续发 voteTask）
                        this.askForVoteTask(votes, VOTES_BACK_OFF_MS);
                    });
                    Timer.getInstance()
                         .addTask(timedTask);
                    taskMap.put(TaskEnum.ASK_FOR_VOTES, timedTask);
                } else {
                    // do nothing
                }
                return null;
            });
        }
    }

    /**
     * 心跳任务
     */
    private void initHeartBeatTask() {
        meta.getClusters()
            .forEach(hanabiNode -> {
                if (!hanabiNode.isLocalNode()) {
                    // 确保和其他选举服务器保持连接
                    ElectClientOperator.getInstance(hanabiNode)
                                       .tryStartWhileDisconnected();

                    // 向其他节点发送拉票请求
                    Optional.ofNullable(ChannelManager.getInstance(ChannelType.ELECT)
                                                      .getChannel(hanabiNode.getServerName()))
                            .ifPresent(channel -> {
                                //                            logger.debug("正向节点 {} [{}:{}] 发送世代 {} 的心跳...", hanabiNode.getServerName(), hanabiNode.getHost(), hanabiNode.getElectionPort(), meta.getGeneration());
                                channel.writeAndFlush(ElectCoder.encodeToByteBuf(ElectProtocolEnum.HEART_BEAT, meta.getHeartBeat()));
                            });
                }
            });

        TimedTask timedTask = new TimedTask(HEART_BEAT_MS, this::initHeartBeatTask);
        Timer.getInstance()
             .addTask(timedTask);
        taskMap.put(TaskEnum.HEART_BEAT, timedTask);
    }

    /**
     * 成为候选者的任务，（重复调用则会取消之前的任务，收到来自leader的心跳包，就可以重置一下这个任务）
     *
     * 没加锁，因为这个任务需要频繁被调用，只要收到leader来的消息就可以调用一下
     */
    private void becomeCandidateAndBeginElectTask(long generation) {
        this.lockSupplier(() -> {
            this.cancelCandidateAndBeginElectTask("正在重置发起下一轮选举的退避时间");

            // The election timeout is randomized to be between 150ms and 300ms.
            long electionTimeout = ELECTION_TIMEOUT_MS + (int) (ELECTION_TIMEOUT_MS * RANDOM.nextFloat());
            TimedTask timedTask = new TimedTask(electionTimeout, () -> this.beginElect(generation));
            Timer.getInstance()
                 .addTask(timedTask);

            taskMap.put(TaskEnum.BECOME_CANDIDATE, timedTask);
            return null;
        });
    }

    /**
     * 取消成为候选者的任务，成为leader，或者调用 {@link #becomeCandidateAndBeginElectTask} （心跳）
     * 时调用，也就是说如果没能成为leader，又会重新进行一次选主，直到成为leader，或者follower。
     */
    private void cancelCandidateAndBeginElectTask(String msg) {
        this.lockSupplier(() -> {
            logger.debug(msg);
            Optional.ofNullable(taskMap.get(TaskEnum.BECOME_CANDIDATE))
                    .ifPresent(TimedTask::cancel);
            return null;
        });
    }

    /**
     * 取消所有的定时任务
     */
    private void cancelAllTask() {
        this.lockSupplier(() -> {
            logger.debug("取消本节点在上个世代的所有定时任务");
            taskMap.values()
                   .forEach(TimedTask::cancel);
            return null;
        });
    }

    public void updateGenWhileReceiveHigherGen(long generation, String msg) {
        this.init(generation, msg);
    }

    /**
     * 生成对应一次操作的id号（用于给其他节点发送日志同步消息，并且得到其ack，以便知道消息是否持久化成功）
     */
    public GenerationAndOffset genOperationId() {
        return this.lockSupplier(() -> {
            if (NodeRole.Leader == meta.getNodeRole()) {

                long gen = meta.getGeneration();

                // 当流水号达到最大时，进行世代的自增，
                if (meta.getOffset() == Long.MAX_VALUE) {
                    logger.warn("流水号 offset 已达最大值，节点将更新自身世代 {} => {}", meta.getGeneration(), meta.getGeneration() + 1);
                    //                    offset = Long.MAX_VALUE - 10;
                    meta.setOffset(0L);
                    gen = meta.generationIncr();
                }

                long offset = meta.offsetIncr();

                return new GenerationAndOffset(gen, offset);
            } else {
                throw new ElectException("不是 Leader 的节点无法生成id号");
            }
        });
    }

    /**
     * 设置本节点的世代和位移，仅有未启动时才可设置，已经启动则无法再设置
     */
    public ElectOperator resetGenerationAndOffset(GenerationAndOffset generationAndOffset) {
        if (startLatch.getCount() > 0) {
            startLatch.countDown();
            meta.setGeneration(generationAndOffset.getGeneration());
            meta.setOffset(generationAndOffset.getOffset());
        }
        return this;
    }

    public void start() {
        startLatch.countDown();
    }

    @Override
    public void run() {
        this.lockSupplier(() -> {
            logger.info("初始化选举控制器 ElectOperator，本节点为 {}", InetSocketAddressConfigHelper.getServerName());
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.debug("初始化选举控制器 启动中");
            this.becomeCandidateAndBeginElectTask(meta.getGeneration());
            return null;
        });
    }
}
