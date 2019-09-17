package com.anur.core.coordinate.apis.recovery

import com.anur.config.InetSocketAddressConfiguration
import com.anur.core.common.Resetable
import com.anur.core.coordinate.apis.driver.ApisManager
import com.anur.core.coordinate.apis.fetch.CoordinateFetcher
import com.anur.core.coordinate.model.RequestProcessor
import com.anur.core.coordinate.operator.CoordinateClientOperator
import com.anur.core.elect.ElectMeta
import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.listener.EventEnum
import com.anur.core.listener.HanabiListener
import com.anur.core.struct.coordinate.FetchResponse
import com.anur.core.struct.coordinate.RecoveryComplete
import com.anur.core.util.TimeUtil
import com.anur.io.hanalog.log.LogManager
import com.anur.io.hanalog.prelog.ByteBufPreLogManager
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer

/**
 * Created by Anur IjuoKaruKas on 4/9/2019
 *
 * 当服务器挂机或者不正常时，需要进行集群日志的恢复
 *
 * 当选主成功后
 *
 * - 所有节点进行coordinate的注册，注册后上报其最大 commit offset
 *
 * - 集群进入恢复状态，直到半数节点上报数据
 *
 * -- 是否达到半数节点上报 => no => 节点一直阻塞，直到有半数节点上报
 *
 * |
 * |
 * V
 * yes
 *
 * - 获取最大的commit，作为 recovery point
 *
 * -- leader 是否达到此 commit 数据 => no => 向拥有此数据的节点进行 fetch
 *
 * -- leader 恢复完毕后。将leader节点改为日志恢复完毕状态
 *
 * -- 回复所有子节点，节点所在世代最大 commitPoint 为多少，子节点删除大于 commit Point 的数据
 *
 * |
 * |
 * V
 *
 * 集群可用
 *
 * //////////////////////////////////////////////////////////////////////////////////
 *
 * 集群可用后连上leader的需要做特殊处理：
 *
 * 需要检查当前世代 的last Offset，进行check，如果与leader不符，则需要truncate后恢复可用。
 */
object LeaderClusterRecoveryManager : Resetable, CoordinateFetcher() {

    override fun reset() {
        writeLocker {
            logger.debug("LeaderClusterRecoveryManager RESET is triggered")
            RecoveryMap.clear()
            recoveryComplete = false
        }
    }

    init {
        /*
         * 当项目选主成功后，子节点需启动协调控制器去连接主节点
         *
         * 将 recoveryComplete 设置为真，表示正在集群正在日志恢复
         */
        HanabiListener.register(EventEnum.CLUSTER_VALID) {
            reset()
            RecoveryTimer = TimeUtil.getTime()
            if (ElectMeta.isLeader) {
                receive(ElectMeta.leader!!, ByteBufPreLogManager.getCommitGAO())
            }
        }

        /*
         * 当集群不可用，则重置所有状态
         */
        HanabiListener.register(EventEnum.CLUSTER_INVALID) {
            reset()
        }
    }

    private val logger = LoggerFactory.getLogger(LeaderClusterRecoveryManager::class.java)

    private var RecoveryTimer: Long = 0

    @Volatile
    private var waitShutting = ConcurrentHashMap<String, GenerationAndOffset>()

    @Volatile
    private var RecoveryMap = ConcurrentHashMap<String, GenerationAndOffset>()

    @Volatile
    private var recoveryComplete = false

    @Volatile
    private var fetchTo: GenerationAndOffset? = null

    fun receive(serverName: String, latestGao: GenerationAndOffset) {
        logger.info("节点 $serverName 提交了其最大进度 $latestGao ")
        RecoveryMap[serverName] = latestGao

        if (!recoveryComplete) {
            if (RecoveryMap.size >= ElectMeta.quorum) {
                var latest: MutableMap.MutableEntry<String, GenerationAndOffset>? = null
                RecoveryMap.entries.forEach(Consumer {
                    if (latest == null || it.value > latest!!.value) {
                        latest = it
                    }
                })

                if (latest!!.value == ByteBufPreLogManager.getCommitGAO()) {
                    val cost = TimeUtil.getTime() - RecoveryTimer
                    logger.info("已有过半节点提交了最大进度，且集群最大进度 ${latest!!.value} 与 Leader 节点相同，集群已恢复，耗时 $cost ms ")
                    shuttingWhileRecoveryComplete()
                } else {
                    val serverName = latest!!.key
                    val GAO = latest!!.value
                    val node = InetSocketAddressConfiguration.getNode(serverName)
                    fetchTo = GAO

                    logger.info("已有过半节点提交了最大进度，集群最大进度于节点 $serverName ，进度为 $GAO ，Leader 将从其同步最新数据")

                    /*
                     * 当连接上子节点，开始日志同步，同步具体逻辑在 {@link #howToConsumeFetchResponse}
                     */
                    HanabiListener.register(EventEnum.COORDINATE_CONNECT_TO, serverName) {
                        startToFetchFrom(serverName)
                    }

                    /*
                     * 当断开子节点，取消同步任务
                     */
                    HanabiListener.register(EventEnum.COORDINATE_DISCONNECT_TO, serverName) {
                        cancelFetchTask()
                    }

                    /*
                     * 当集群不可用，清除注册 COORDINATE_CONNECT 的注册
                     */
                    HanabiListener.register(EventEnum.CLUSTER_INVALID) {
                        HanabiListener.clear(EventEnum.COORDINATE_CONNECT_TO, serverName)
                        HanabiListener.clear(EventEnum.COORDINATE_DISCONNECT_TO, serverName)
                        CoordinateClientOperator.getInstance(node).shutDown()
                        fetchTo = null
                    }

                    /*
                     * 真正开始连接子节点
                     */
                    CoordinateClientOperator.getInstance(node).tryStartWhileDisconnected()
                }
            }
        }

        sendRecoveryComplete(serverName, latestGao)
    }

    private fun shuttingWhileRecoveryComplete() {
        recoveryComplete = true
        waitShutting.entries.forEach(Consumer { sendRecoveryComplete(it.key, it.value) })
        HanabiListener.onEvent(EventEnum.RECOVERY_COMPLETE)
    }

    private fun sendRecoveryComplete(serverName: String, latestGao: GenerationAndOffset) {
        if (recoveryComplete) {
            doSendRecoveryComplete(serverName, latestGao)
        } else {
            waitShutting[serverName] = latestGao
        }
    }

    private fun doSendRecoveryComplete(serverName: String, latestGao: GenerationAndOffset) {
        val GAO = GenerationAndOffset(latestGao.generation, LogManager.loadGenLog(latestGao.generation)!!.currentOffset)
        ApisManager.send(serverName, RecoveryComplete(GAO), RequestProcessor.REQUIRE_NESS)
    }

    override fun howToConsumeFetchResponse(fetchFrom: String, fetchResponse: FetchResponse) {
        logger.trace("收到节点 {} 返回的 FETCH_RESPONSE", fetchFrom)

        ElectMeta.takeIf { !it.isLeader }?.run { logger.error("出现了不应该出现的情况！喵喵锤！") }
        val read = fetchResponse.read()
        val iterator = read.iterator()

        val gen = fetchResponse.generation
        val fetchToGen = fetchTo!!.generation

        var start: Long? = null
        var end: Long? = null

        iterator.forEach {

            if (start == null) start = it.offset
            end = it.offset

            LogManager
                .append(gen, it.offset, it.operation)
            ByteBufPreLogManager.cover(GenerationAndOffset(gen,end!!))// 实际上这是兼容用的 = =，因为父类模板方法从这里取的数据去 fetch

            if (gen == fetchToGen) {
                val offset = it.offset
                val fetchToOffset = fetchTo!!.offset
                if (offset == fetchToOffset) {// 如果已经同步完毕，则通知集群同步完成
                    cancelFetchTask()
                    shuttingWhileRecoveryComplete()
                }
            }
        }

        ByteBufPreLogManager.cover(GenerationAndOffset(gen, end!!))
        logger.debug("集群日志恢复：追加 gen = {$gen} offset-start {$start} end {$end} 的日志段完毕")
    }
}