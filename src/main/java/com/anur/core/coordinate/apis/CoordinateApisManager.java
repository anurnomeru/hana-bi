package com.anur.core.coordinate.apis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.CoordinateConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.coordinate.CoordinateClientOperator;
import com.anur.core.coordinate.model.RequestProcessor;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.lock.ReentrantReadWriteLocker;
import com.anur.core.struct.coordinate.FetchResponse;
import com.anur.core.struct.coordinate.Fetcher;
import com.anur.io.store.OffsetManager;
import com.anur.io.store.prelog.ByteBufPreLogManager;
import com.anur.timewheel.TimedTask;
import com.anur.timewheel.Timer;

/**
 * Created by Anur IjuoKaruKas on 2019/3/26
 *
 * 日志一致性控制器
 */
public class CoordinateApisManager extends ReentrantReadWriteLocker {

    private static volatile CoordinateApisManager INSTANCE;

    private Logger logger = LoggerFactory.getLogger(CoordinateApisManager.class);

    /**
     * 集群是否可用，此状态由 {@link ElectOperator#registerWhenClusterValid 和 {@link ElectOperator#registerWhenClusterInvalid}} 共同维护
     */
    private volatile boolean clusterValid = false;

    /**
     * Leader 节点
     */
    private volatile String leader = null;

    /**
     * 当前集群内有哪些机器
     */
    private volatile List<HanabiNode> clusters = null;

    /**
     * 是否 Leader
     */
    private boolean isLeader = false;

    /**
     * 作为 Leader 时有效，日志需要拿到 n 个 commit 才可以提交
     */
    private volatile int validCommitCountNeed = Integer.MAX_VALUE;

    /**
     * 作为 Leader 时有效，维护了每个节点的 fetch 进度
     */
    private ConcurrentSkipListMap<GenerationAndOffset, Set<String>> fetchMap = new ConcurrentSkipListMap<>();

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 fetch
     */
    private Map<String, GenerationAndOffset> nodeFetchMap = new HashMap<>();

    /**
     * 作为 Leader 时有效，维护了每个节点的 commit 进度
     */
    private ConcurrentSkipListMap<GenerationAndOffset, Set<String>> commitMap = new ConcurrentSkipListMap<>();

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 commit
     */
    private Map<String, GenerationAndOffset> nodeCommitMap = new HashMap<>();

    /**
     * 作为 Follower 时有效，此任务不断从 Leader 节点获取 PreLog
     */
    private TimedTask fetchPreLogTask = null;

    /**
     * Fetch 锁
     */
    private Lock fetchLock = new ReentrantLock();

    public static CoordinateApisManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (CoordinateApisManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new CoordinateApisManager();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * 如何消费 Fetch response
     */
    private Consumer<FetchResponse> CONSUME_FETCH_RESPONSE = fetchResponse -> {
        readLockSupplier(() -> {
            logger.debug("收到 Leader {} 返回的 FETCH_RESPONSE", leader);

            if (isLeader) {
                logger.error("出现了不应该出现的情况！");
            }

            if (fetchResponse.getFileOperationSetSize() == 0) {
                return null;
            }

            ByteBufPreLogManager.getINSTANCE()
                                .append(fetchResponse.getGeneration(), fetchResponse.read());
            return null;
        });
    };

    /**
     * 定时 Fetch 消息
     */
    public void sendFetchPreLog() {
        fetchLock.lock();
        try {
            if (fetchPreLogTask != null) {
                if (!fetchPreLogTask.isCancel()) {
                    if (ApisManager.getINSTANCE()
                                   .send(
                                       leader,
                                       new Fetcher(
                                           ByteBufPreLogManager.getINSTANCE()
                                                               .getPreLogGAO()
                                       ),
                                       new RequestProcessor(byteBuffer ->
                                           CONSUME_FETCH_RESPONSE.accept(new FetchResponse(byteBuffer)),
                                           () -> {
                                               logger.debug("rebuild fetchPreLogTask task");
                                               fetchPreLogTask = new TimedTask(CoordinateConfigHelper.getFetchBackOfMs(), this::sendFetchPreLog);
                                               Timer.getInstance()
                                                    .addTask(fetchPreLogTask);
                                           }))) {
                    }
                }
            }
        } finally {
            fetchLock.unlock();
        }
    }

    /**
     * Follower 向 Leader 提交拉取到的最大的 GAO
     *
     * 如果某个最大的 GAO 已经达到了 commit 条件，将返回此 GAO。
     */
    public GenerationAndOffset fetchReport(String node, GenerationAndOffset GAO) {
        GenerationAndOffset GAOLatest = OffsetManager.getINSTANCE()
                                                     .load();
        if (!isLeader) {
            logger.error("出现了不应该出现的情况！");
            return GAOLatest;
        }

        if (GAOLatest.compareTo(GAO) > 0) {// 小于已经 commit 的 GAO 无需记录
            return GAOLatest;
        }

        GenerationAndOffset GAOFetchBefore = readLockSupplier(() -> nodeFetchMap.get(node));
        if (GAOFetchBefore != null && GAOFetchBefore.compareTo(GAO) >= 0) {// 小于之前提交记录的无需记录
            return GAOLatest;
        }

        return writeLockSupplier(() -> {
            if (GAOFetchBefore != null) {// 移除之前的 commit 记录
                logger.debug("节点 {} fetch 进度由 {} 更新到了进度 {}", node, GAOFetchBefore.toString(), GAO.toString());
                fetchMap.get(GAOFetchBefore)
                        .remove(node);
            } else {
                logger.debug("节点 {} 已经 fetch 更新到了进度 {}", node, GAO.toString());
            }

            nodeFetchMap.put(node, GAO);// 更新新的 commit 记录
            fetchMap.compute(GAO, (generationAndOffset, strings) -> { // 更新新的 commit 记录
                if (strings == null) {
                    strings = new HashSet<>();
                }
                strings.add(node);
                return strings;
            });

            GenerationAndOffset approach = null;
            for (Entry<GenerationAndOffset, Set<String>> entry : fetchMap.entrySet()) {
                if (entry.getValue()
                         .size() + 1 >= validCommitCountNeed) {// +1 为自己的一票
                    approach = entry.getKey();
                }
            }

            if (approach == null) {
                return GAOLatest;
            } else {
                logger.info("进度 {} 已可提交 ~ 已经拟定 approach，半数节点同意则进行 commit", GAO.toString());
                return approach;
            }
        });
    }

    public void commitReport(String node, GenerationAndOffset commitGAO) {
        GenerationAndOffset GAOLatest = OffsetManager.getINSTANCE()
                                                     .load();

        if (GAOLatest.compareTo(commitGAO) > 0) {// 小于已经 commit 的 GAO 直接无视
            return;
        }

        GenerationAndOffset GAOCommitBefore = readLockSupplier(() -> nodeCommitMap.get(node));
        if (GAOCommitBefore != null && GAOCommitBefore.compareTo(commitGAO) >= 0) {// 小于之前提交记录的无需记录
            return;
        }

        writeLockSupplier(() -> {
            if (GAOCommitBefore != null) {// 移除之前的 commit 记录
                logger.debug("节点 {} 已经 commit 进度由 {} 更新到了进度 {}", node, GAOCommitBefore.toString(), commitGAO.toString());
                commitMap.get(GAOCommitBefore)
                         .remove(node);
            } else {
                logger.debug("节点 {} 已经 fetch 更新到了进度 {}", node, commitGAO.toString());
            }

            nodeCommitMap.put(node, commitGAO);// 更新新的 commit 记录
            commitMap.compute(commitGAO, (generationAndOffset, strings) -> { // 更新新的 commit 记录
                if (strings == null) {
                    strings = new HashSet<>();
                }
                strings.add(node);
                return strings;
            });

            GenerationAndOffset approach = null;
            for (Entry<GenerationAndOffset, Set<String>> entry : commitMap.entrySet()) {
                if (entry.getValue()
                         .size() + 1 >= validCommitCountNeed) {// +1 为自己的一票
                    approach = entry.getKey();
                }
            }

            if (approach == null) {
                return GAOLatest;
            } else {
                logger.info("进度 {} 已经完成 commit ~", commitGAO.toString());
                OffsetManager.getINSTANCE()
                             .cover(commitGAO);
                return approach;
            }
        });
    }

    public CoordinateApisManager() {
        ElectOperator.getInstance()
                     .registerWhenClusterValid(
                         cluster -> {
                             leader = cluster.getLeader();
                             isLeader = InetSocketAddressConfigHelper.getServerName()
                                                                     .equals(leader);

                             if (isLeader) {
                                 clusterValid = true;
                                 clusters = cluster.getClusters();
                                 validCommitCountNeed = clusters.size() / 2 + 1;
                             } else {
                                 // 当集群可用时，连接协调 leader
                                 CoordinateClientOperator client = CoordinateClientOperator.getInstance(InetSocketAddressConfigHelper.getNode(cluster.getLeader()));
                                 client.registerWhenConnectToLeader(() -> {
                                     fetchLock.lock();
                                     try {
                                         fetchPreLogTask = new TimedTask(CoordinateConfigHelper.getFetchBackOfMs(), this::sendFetchPreLog);
                                         Timer.getInstance()
                                              .addTask(fetchPreLogTask);
                                         logger.debug("init fetchPreLogTask task");
                                     } finally {
                                         fetchLock.unlock();
                                     }
                                     clusterValid = true;
                                 });

                                 client.registerWhenDisconnectToLeader(() -> {
                                     fetchLock.lock();
                                     try {
                                         Optional.ofNullable(fetchPreLogTask)
                                                 .ifPresent(TimedTask::cancel);
                                         logger.debug("cancel fetchPreLogTask task");
                                     } finally {
                                         fetchLock.unlock();
                                     }
                                 });
                                 client.tryStartWhileDisconnected();
                             }
                         });

        ElectOperator.getInstance()
                     .registerWhenClusterInvalid(
                         () -> {
                             fetchLock.lock();
                             try {
                                 Optional.ofNullable(fetchPreLogTask)
                                         .ifPresent(TimedTask::cancel);
                                 logger.debug("cancel fetchPreLogTask task");
                             } finally {
                                 fetchLock.unlock();
                             }

                             clusterValid = false;
                             isLeader = false;
                             clusters = null;
                             leader = null;
                             validCommitCountNeed = Integer.MAX_VALUE;

                             // 当集群不可用时，与协调 leader 断开连接
                             CoordinateClientOperator.shutDownInstance("集群已不可用，与协调 Leader 断开连接");
                             ApisManager.getINSTANCE()
                                        .reboot();
                         });
    }
}
