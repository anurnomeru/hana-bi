package com.anur.core.coordinate;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.model.GenerationAndOffset;

/**
 * Created by Anur IjuoKaruKas on 4/16/2019
 *
 * 维护协调节点的所有变量，并负责协调各个组件进行工作，通过注册调用链的方式来传递状态
 */
public class CoordinateMetaData {

    private volatile static CoordinateMetaData INSTANCE;

    public CoordinateMetaData getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (CoordinateMetaData.class) {
                if (INSTANCE == null) {
                    INSTANCE = new CoordinateMetaData();
                }
            }
        }
        return INSTANCE;
    }

    private CoordinateMetaData() {
        ElectOperator.getInstance()
                     .registerWhenClusterVotedALeader(cluster -> {
                         leader = cluster.getLeader();
                         isLeader = InetSocketAddressConfigHelper.getServerName()
                                                                 .equals(leader);
                         clusters = cluster.getClusters();
                         validCommitCountNeed = clusters.size() / 2 + 1;

                         if (!isLeader) {
                             CoordinateClientOperator.getInstance(InetSocketAddressConfigHelper.getNode(cluster.getLeader()))
                                                     .tryStartWhileDisconnected();
                         }
                     });

        ElectOperator.getInstance()
                     .registerWhenClusterInvalid(() -> {
                         clusterValid = false;
                         isLeader = false;
                         clusters = null;
                         leader = null;
                         validCommitCountNeed = Integer.MAX_VALUE;
                         fetchMap = new ConcurrentSkipListMap<>();
                         commitMap = new ConcurrentSkipListMap<>();
                         nodeFetchMap = new ConcurrentHashMap<>();
                         nodeCommitMap = new ConcurrentHashMap<>();

                         // 当集群不可用时，与协调 leader 断开连接
                         CoordinateClientOperator.shutDownInstance("集群已不可用，与协调 Leader 断开连接");
                     });
    }

    //    private volatile CoordinateState coordinateState = CoordinateS;

    /**
     * 集群是否可用，此状态由 {@link ElectOperator#registerWhenClusterVotedALeader 和 {@link ElectOperator#registerWhenClusterInvalid}} 共同维护
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
    private volatile Map<String, GenerationAndOffset> nodeFetchMap = new ConcurrentHashMap<>();

    /**
     * 作为 Leader 时有效，维护了每个节点的 commit 进度
     */
    private volatile ConcurrentSkipListMap<GenerationAndOffset, Set<String>> commitMap = new ConcurrentSkipListMap<>();

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 commit
     */
    private volatile Map<String, GenerationAndOffset> nodeCommitMap = new ConcurrentHashMap<>();

    public boolean isClusterValid() {
        return clusterValid;
    }

    public String getLeader() {
        return leader;
    }

    public List<HanabiNode> getClusters() {
        return clusters;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public int getValidCommitCountNeed() {
        return validCommitCountNeed;
    }

    public ConcurrentSkipListMap<GenerationAndOffset, Set<String>> getFetchMap() {
        return fetchMap;
    }

    public Map<String, GenerationAndOffset> getNodeFetchMap() {
        return nodeFetchMap;
    }

    public ConcurrentSkipListMap<GenerationAndOffset, Set<String>> getCommitMap() {
        return commitMap;
    }

    public Map<String, GenerationAndOffset> getNodeCommitMap() {
        return nodeCommitMap;
    }
}
