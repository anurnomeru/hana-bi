package com.anur.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.coordinate.CoordinateClientOperator;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.lock.ReentrantReadWriteLocker;
import com.anur.core.coordinate.InFlightRequestManager;
import com.anur.io.store.OffsetManager;

/**
 * Created by Anur IjuoKaruKas on 2019/3/26
 *
 * 日志一致性控制器
 */
public class ConsistentManager extends ReentrantReadWriteLocker {

    private static volatile ConsistentManager INSTANCE;

    private volatile boolean clusterValid = false;

    private volatile List<HanabiNode> clusters = null;

    private volatile int validCommitCountNeed = Integer.MAX_VALUE;

    private ConcurrentSkipListMap<GenerationAndOffset, Set<String>> offsetCommitMap = new ConcurrentSkipListMap<>();

    private Map<String, GenerationAndOffset> nodeCommitMap = new HashMap<>();

    private boolean isLeader = false;

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
                     .registerWhenClusterValid(cluster -> writeLockSupplier(() -> {
                         clusterValid = true;
                         isLeader = InetSocketAddressConfigHelper.getServerName()
                                                                 .equals(cluster.getLeader());

                         if (isLeader) {
                             clusters = cluster.getClusters();
                             validCommitCountNeed = clusters.size() / 2 + 1;
                         } else {
                             CoordinateClientOperator.getInstance(InetSocketAddressConfigHelper.getNode(cluster.getLeader()))
                                                     .tryStartWhileDisconnected();
                         }

                         return null;
                     }));

        ElectOperator.getInstance()
                     .registerWhenClusterInvalid(this::disabled);
    }

    private void disabled() {
        writeLockSupplier(() -> {
            clusterValid = false;
            isLeader = false;
            clusters = null;
            validCommitCountNeed = Integer.MAX_VALUE;
            CoordinateClientOperator.shutDownInstance("集群已不可用，与协调 Leader 断开连接");
            InFlightRequestManager.getINSTANCE()
                                  .reboot();
            return null;
        });
    }

    public GenerationAndOffset commitAck(String node, GenerationAndOffset GAO) {
        return readLockSupplier(() -> {
            GenerationAndOffset GAOLatest = OffsetManager.getINSTANCE()
                                                         .load();
            if (GAOLatest.compareTo(GAO) > 0) {// 小于已经 commit 的 GAO 无需记录
                return GAOLatest;
            }

            GenerationAndOffset GAOCommitBefore = nodeCommitMap.get(node);
            if (GAOCommitBefore != null && GAOCommitBefore.compareTo(GAO) > 0) {// 小于之前提交记录的无需记录
                return GAOLatest;
            }

            writeLockSupplier(() -> {
                if (GAOCommitBefore != null) {
                    offsetCommitMap.get(GAOCommitBefore)
                                   .remove(node);
                }

                nodeCommitMap.put(node, GAO);
                offsetCommitMap.compute(GAO, (generationAndOffset, strings) -> {
                    if (strings == null) {
                        strings = new HashSet<>();
                    }
                    strings.add(node);
                    return strings;
                });

                GenerationAndOffset approach = null;
                for (Entry<GenerationAndOffset, Set<String>> entry : offsetCommitMap.entrySet()) {
                    if (entry.getValue()
                             .size() >= validCommitCountNeed) {
                        approach = entry.getKey();
                    }
                }

                return approach;
            });

            return null;
        });
    }
}
