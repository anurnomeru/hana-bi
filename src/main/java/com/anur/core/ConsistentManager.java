package com.anur.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.command.modle.Fetcher;
import com.anur.core.coordinate.CoordinateClientOperator;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.lock.ReentrantReadWriteLocker;
import com.anur.core.coordinate.sender.InFlightRequestManager;
import com.anur.io.store.OffsetManager;
import com.anur.timewheel.TimedTask;

/**
 * Created by Anur IjuoKaruKas on 2019/3/26
 *
 * 日志一致性控制器
 */
public class ConsistentManager extends ReentrantReadWriteLocker {

    private static volatile ConsistentManager INSTANCE;

    private Logger logger = LoggerFactory.getLogger(ConsistentManager.class);

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

    public void sendFetchPreLog() {
        if (fetchPreLogTask != null) {
            if (!fetchPreLogTask.isCancel()) {
                InFlightRequestManager.getINSTANCE()
                                      .send(leader,new Fetcher());
            }
        }
    }

    /**
     * Follower 向 Leader 提交拉取到的最大的 GAO
     *
     * 如果某个最大的 GAO 已经达到了 commit 条件，将返回此 GAO。
     */
    public GenerationAndOffset fetchReport(String node, GenerationAndOffset GAO) {
        logger.debug("收到了来自节点 {} 的 fetch 进度提交，此阶段进度为 {}", node, GAO.toString());

        GenerationAndOffset canCommit = readLockSupplier(() -> {
            GenerationAndOffset GAOLatest = OffsetManager.getINSTANCE()
                                                         .load();
            if (GAOLatest.compareTo(GAO) > 0) {// 小于已经 commit 的 GAO 无需记录
                return GAOLatest;
            }

            GenerationAndOffset GAOCommitBefore = nodeFetchMap.get(node);
            if (GAOCommitBefore != null && GAOCommitBefore.compareTo(GAO) > 0) {// 小于之前提交记录的无需记录
                return GAOLatest;
            }

            writeLockSupplier(() -> {
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

                return approach;
            });

            return null;
        });

        logger.debug("进度 {} 已可提交 ~", node, GAO.toString());
        return canCommit;
    }

    public static ConsistentManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (ConsistentManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ConsistentManager();
                }
            }
        }
        return INSTANCE;
    }

    public ConsistentManager() {
        ElectOperator.getInstance()
                     .registerWhenClusterValid(
                         cluster -> writeLockSupplier(() -> {
                             clusterValid = true;
                             leader = cluster.getLeader();
                             isLeader = InetSocketAddressConfigHelper.getServerName()
                                                                     .equals(leader);
                             if (isLeader) {
                                 clusters = cluster.getClusters();
                                 validCommitCountNeed = clusters.size() / 2 + 1;
                             } else {

                                 // 当集群可用时，连接协调 leader
                                 CoordinateClientOperator.getInstance(InetSocketAddressConfigHelper.getNode(cluster.getLeader()))
                                                         .tryStartWhileDisconnected();
                             }
                             return null;
                         }));

        ElectOperator.getInstance()
                     .registerWhenClusterInvalid(
                         () -> writeLockSupplier(() -> {
                             clusterValid = false;
                             isLeader = false;
                             clusters = null;
                             leader = null;
                             validCommitCountNeed = Integer.MAX_VALUE;

                             // 当集群不可用时，与协调 leader 断开连接
                             CoordinateClientOperator.shutDownInstance("集群已不可用，与协调 Leader 断开连接");
                             InFlightRequestManager.getINSTANCE()
                                                   .reboot();
                             return null;
                         }));
    }
}
