package com.anur.core.elect

import com.anur.config.InetSocketAddressConfigHelper
import com.anur.core.elect.constant.NodeRole
import com.anur.core.elect.model.HeartBeat
import com.anur.core.elect.model.Votes
import com.anur.core.listener.EventEnum
import com.anur.core.listener.HanabiListener
import com.anur.core.util.TimeUtil
import com.anur.exception.ApplicationConfigException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2019/7/8
 *
 * 选举相关的元数据信息都保存在这里
 */
object ElectMeta {

    val logger: Logger = LoggerFactory.getLogger(ElectMeta.javaClass);

    /**
     * 该投票箱的世代信息，如果一直进行选举，一直能达到 [.ELECTION_TIMEOUT_MS]，而选不出 Leader ，也需要15年，generation才会不够用，如果
     * generation 的初始值设置为 Long.Min （现在是0，则可以撑30年，所以完全呆胶布）
     */
    @Volatile
    var generation: Long = 0L

    /**
     * 现在集群的leader是哪个节点
     */
    @Volatile
    var leader: String? = null

    /**
     * 是否 Leader
     */
    @Volatile
    var isLeader = false

    /**
     * 流水号，用于生成 id，集群内每一次由 Leader 发起的关键操作都会生成一个id [.genOperationId] ()}，其中就需要自增 offset 号
     */
    @Volatile
    var offset: Long = 0L

    /**
     * 投票箱
     */
    @Volatile
    var box: MutableMap<String/* serverName */, Boolean> = mutableMapOf()

    /**
     * 投票给了谁的投票记录
     */
    @Volatile
    var voteRecord: Votes? = null

    /**
     * 缓存一份集群信息，因为集群信息是可能变化的，我们要保证在一次选举中，集群信息是不变的
     */
    @Volatile
    var clusters: List<InetSocketAddressConfigHelper.HanabiNode>? = null

    /**
     * 法定人数
     */
    @Volatile
    var quorom: Int = Int.MAX_VALUE

    /**
     * 当前节点的角色
     */
    @Volatile
    var nodeRole: NodeRole = NodeRole.Follower


    /**
     * 选举是否已经进行完
     */
    @Volatile
    private var electionCompleted: Boolean = false

    /**
     * 心跳内容
     */
    val heartBeat: HeartBeat = HeartBeat(InetSocketAddressConfigHelper.getServerName() ?: throw ApplicationConfigException("未正确配置 server.name 或 client.addr"))

    /**
     * 仅用于统计选主用了多长时间
     */
    var beginElectTime: Long = 0

    fun generationIncr(): Long {
        return ++generation
    }

    fun offsetIncr(): Long {
        return ++offset
    }

    /**
     * 当集群选举状态变更时调用
     */
    fun electionStateChanged(electionCompleted: Boolean): Boolean {
        val changed = electionCompleted != this.electionCompleted

        if (changed) {
            this.electionCompleted = electionCompleted
            if (electionCompleted) HanabiListener.onEvent(EventEnum.CLUSTER_VALID)
            else HanabiListener.onEvent(EventEnum.CLUSTER_INVALID)
        }

        return changed
    }

    /**
     * 成为候选者
     */
    fun becomeCandidate(): Boolean {
        return if (nodeRole == NodeRole.Follower) {
            logger.info("本节点角色由 {} 变更为 {}", nodeRole.name, NodeRole.Candidate.name)
            nodeRole = NodeRole.Candidate
            isLeader = false
            electionStateChanged(false)
            true
        } else {
            logger.debug("本节点的角色已经是 {} ，无法变更为 {}", nodeRole.name, NodeRole.Candidate.name)
            false
        }
    }

    /**
     * 成为追随者
     */
    fun becomeFollower() {
        if (nodeRole !== NodeRole.Follower) {
            logger.info("本节点角色由 {} 变更为 {}", nodeRole.name, NodeRole.Follower.name)
            nodeRole = NodeRole.Follower
            isLeader = false
        }
    }

    /**
     * 当选票大于一半以上时调用这个方法，如何去成为一个leader
     */
    fun becomeLeader() {
        val becomeLeaderCostTime = TimeUtil.getTime() - beginElectTime;
        beginElectTime = 0L

        logger.info("本节点 {} 在世代 {} 角色由 {} 变更为 {} 选举耗时 {} ms，并开始向其他节点发送心跳包 ......",
            InetSocketAddressConfigHelper.getServerName(), generation, nodeRole
            .name, NodeRole.Leader.name, becomeLeaderCostTime)

        nodeRole = NodeRole.Leader
        leader = InetSocketAddressConfigHelper.getServerName()
        isLeader = true
        electionStateChanged(true)
    }
}