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
    private ConcurrentSkipListMap<GenerationAndOffset, Set<String>> offsetFetchMap = new ConcurrentSkipListMap<>();

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 fetch
     */
    private Map<String, GenerationAndOffset> nodeFetchMap = new HashMap<>();

    /**
     * 作为 Follower 时有效，此任务不断从 Leader 节点获取 PreLog
     */
    private TimedTask fetchPreLogTask = null;

    /**
     * Fetch 锁
     */
    private Lock fetchLock = new ReentrantLock();

    /**
     * 如何消费 Fetch response
     */
    private Consumer<FetchResponse> CONSUME_FETCH_RESPONSE = fetchResponse -> {
        System.out.println("12312333333333333333333333");
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
                    GenerationAndOffset GAO = ByteBufPreLogManager.getINSTANCE()
                                                                  .getPreLogOffset();
                    if (InFlightApisManager.getINSTANCE()
                                           .send(
                                               leader,
                                               new Fetcher(GAO.next()),
                                               new RequestProcessor(byteBuffer ->
                                                   CONSUME_FETCH_RESPONSE.accept(new FetchResponse(byteBuffer)),
                                                   () -> {
                                                       System.out.println("111111111111111111111111111111111");
                                                       fetchPreLogTask = new TimedTask(CoordinateConfigHelper.getFetchBackOfMs(), this::sendFetchPreLog);
                                                       System.out.println("111111111111111111111111111111111");
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
        logger.debug("收到了来自节点 {} 的 fetch 进度提交，此阶段进度为 {}", node, GAO.toString());

        if (!isLeader) {
            logger.error("出现了不应该出现的情况！");
        }

        GenerationAndOffset GAOLatest = OffsetManager.getINSTANCE()
                                                     .load();
        if (GAOLatest.compareTo(GAO) > 0) {// 小于已经 commit 的 GAO 无需记录
            return GAOLatest;
        }

        GenerationAndOffset GAOCommitBefore = readLockSupplier(() -> nodeFetchMap.get(node));
        if (GAOCommitBefore != null && GAOCommitBefore.compareTo(GAO) > 0) {// 小于之前提交记录的无需记录
            return GAOLatest;
        }

        GenerationAndOffset canCommit = writeLockSupplier(() -> {
            if (GAOCommitBefore != null) {// 移除之前的 commit 记录
                offsetFetchMap.get(GAOCommitBefore)
                              .remove(node);
            }

            nodeFetchMap.put(node, GAO);// 更新新的 commit 记录
            offsetFetchMap.compute(GAO, (generationAndOffset, strings) -> { // 更新新的 commit 记录
                if (strings == null) {
                    strings = new HashSet<>();
                }
                strings.add(node);
                return strings;
            });

            GenerationAndOffset approach = null;
            for (Entry<GenerationAndOffset, Set<String>> entry : offsetFetchMap.entrySet()) {
                if (entry.getValue()
                         .size() >= validCommitCountNeed) {
                    approach = entry.getKey();
                }
            }

            return null;
        });

        logger.debug("进度 {} 已可提交 ~", node, GAO.toString());
        return canCommit;
    }

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
                                     System.out.println("registerWhenConnectToLeader");
                                     fetchLock.lock();
                                     try {
                                         fetchPreLogTask = new TimedTask(CoordinateConfigHelper.getFetchBackOfMs(), this::sendFetchPreLog);
                                         Timer.getInstance()
                                              .addTask(fetchPreLogTask);
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
                             InFlightApisManager.getINSTANCE()
                                                .reboot();
                         });
    }
}
