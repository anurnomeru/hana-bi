package com.anur.core.coordinate.apis.recovery

import com.anur.config.InetSocketAddressConfiguration
import com.anur.core.coordinate.apis.driver.ApisManager
import com.anur.core.coordinate.model.RequestProcessor
import com.anur.core.coordinate.operator.CoordinateClientOperator
import com.anur.core.elect.ElectMeta
import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.listener.EventEnum
import com.anur.core.listener.HanabiListener
import com.anur.core.struct.coordinate.RecoveryComplete
import com.anur.core.struct.coordinate.RecoveryReporter
import com.anur.engine.EngineFacade
import com.anur.io.hanalog.log.CommitProcessManager
import com.anur.io.hanalog.log.LogManager
import com.anur.io.hanalog.prelog.ByteBufPreLogManager
import org.slf4j.LoggerFactory
import java.util.function.Consumer

/**
 * Created by Anur IjuoKaruKas on 4/9/2019
 *
 * 当服务器挂机或者不正常时，需要进行集群日志的恢复
 *
 * 当选主成功后
 *
 * - 所有节点进行coordinate的注册，注册时上报其最大 commit offset
 *
 * - 进行 recovery waiting n sec，直到所有节点上报数据
 *
 * -- 是否达到半数节点上报 => no => 节点一直阻塞，直到有半数节点上报
 *
 * |
 * |
 * V
 * yes
 *
 * - 获取最大的commit，作为 recovery point，最小的 commit 则作为 commit GAO
 *
 * -- leader 是否达到此 commit 数据 => no => 向拥有此数据的节点进行 fetch
 *
 * -- 下发指令，删除大于此 recovery point 的数据（针对前leader）
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
object FollowerClusterRecoveryManager {

    private val logger = LoggerFactory.getLogger(FollowerClusterRecoveryManager::class.java)

    init {
        /*
         * 当子节点检测不到 Leader 的心跳后，断开协调控制器
         */
        HanabiListener.register(EventEnum.CLUSTER_INVALID) {
            if (!ElectMeta.isLeader) {
                CoordinateClientOperator.getInstance(InetSocketAddressConfiguration.getNode(ElectMeta.leader)).shutDown()
            }
        }

        /*
         * 当项目选主成功后，子节点需启动协调控制器去连接主节点
         */
        HanabiListener.register(EventEnum.CLUSTER_VALID) {
            if (!ElectMeta.isLeader) {
                CoordinateClientOperator.getInstance(InetSocketAddressConfiguration.getNode(ElectMeta.leader)).tryStartWhileDisconnected()
            }
        }

        /*
         * 当连接主节点成功后，发送当前最大 GAO
         *
         * 1，如果之前是leader，要抛弃一些消息
         *
         * 2，发送最大 GAO
         */
        CommitProcessManager.discardInvalidMsg()

        HanabiListener.register(EventEnum.COORDINATE_CONNECT_TO_LEADER) {

            // 在连接到leader之后,首先将 GAO 置为不可用, 使得数据引擎停止消费
            EngineFacade.coverCommittedProjectGenerationAndOffset(GenerationAndOffset.INVALID)
            ApisManager.send(ElectMeta.leader!!, RecoveryReporter(ByteBufPreLogManager.getCommitGAO()),
                RequestProcessor(Consumer {
                    val recoveryComplete = RecoveryComplete(it)
                    val clusterGAO = recoveryComplete.getCommited()
                    val localGAO = ByteBufPreLogManager.getCommitGAO()
                    if (localGAO > clusterGAO) {
                        logger.debug("当前世代集群日志最高为 $clusterGAO ，比本地 $localGAO 小，故需删除大于集群日志的所有日志")
                        LogManager.discardAfter(clusterGAO)
                    }

                    logger.info("集群已经恢复正常，开始通知 Fetcher 进行日志同步")

                    /*
                     * 当集群同步完毕，通知 RECOVERY_COMPLETE
                     */
                    HanabiListener.onEvent(EventEnum.RECOVERY_COMPLETE)
                }, null))
        }
    }
}
