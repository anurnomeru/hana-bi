package com.anur.core.coordinate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.model.GenerationAndOffset;

/**
 * Created by Anur IjuoKaruKas on 4/16/2019
 *
 * 维护协调节点的所有变量
 */
public class CoordinateMetaData {

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
    private volatile boolean isLeader = false;

    /**
     * 作为 Leader 时有效，日志需要拿到 n 个 commit 才可以提交
     */
    private volatile int validCommitCountNeed = Integer.MAX_VALUE;

    /**
     * 作为 Leader 时有效，维护了每个节点的 fetch 进度
     */
    private volatile ConcurrentSkipListMap<GenerationAndOffset, Set<String>> fetchMap = new ConcurrentSkipListMap<>();

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 fetch
     */
    private volatile Map<String, GenerationAndOffset> nodeFetchMap = new HashMap<>();

    /**
     * 作为 Leader 时有效，维护了每个节点的 commit 进度
     */
    private volatile ConcurrentSkipListMap<GenerationAndOffset, Set<String>> commitMap = new ConcurrentSkipListMap<>();

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 commit
     */
    private volatile Map<String, GenerationAndOffset> nodeCommitMap = new HashMap<>();
}
