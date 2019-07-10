package com.anur.core.coordinate.apis

import com.anur.config.InetSocketAddressConfigHelper
import com.anur.core.common.Resetable
import com.anur.core.coordinate.apis.driver.ApisManager
import com.anur.core.coordinate.model.RequestProcessor
import com.anur.core.coordinate.operator.CoordinateClientOperator
import com.anur.core.elect.ElectMeta
import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.listener.EventEnum
import com.anur.core.listener.HanabiListener
import com.anur.core.struct.coordinate.RecoveryReporter
import com.anur.core.util.TimeUtil
import com.anur.io.store.prelog.ByteBufPreLogManager
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
object ClusterRecoveryManager : Resetable {

    override fun reset() {
        RecoveryMap.clear()
        recoveryComplete = false
    }

    init {

        /*
         * 当项目选主成功后，子节点需启动协调控制器去连接主节点
         *
         * 将 recoveryComplete 设置为真，表示正在集群正在日志恢复
         */
        HanabiListener.register(EventEnum.CLUSTER_VALID) {
            if (!ElectMeta.isLeader) {
                CoordinateClientOperator.getInstance(InetSocketAddressConfigHelper.getNode(ElectMeta.leader)).tryStartWhileDisconnected()
            } else {
                RecoveryMap[ElectMeta.leader!!] = ByteBufPreLogManager.getINSTANCE().commitGAO
            }

            RecoveryTimeCounter = TimeUtil.getTime()
            reset()
        }

        HanabiListener.register(EventEnum.CLUSTER_INVALID) {
            reset()
        }

        HanabiListener.register(EventEnum.COORDINATE_CONNECT_TO_LEADER) {
            sendLatestCommitGao()
        }
    }

    private val logger = LoggerFactory.getLogger(ClusterRecoveryManager::class.java)

    private val RecoveryMap = mutableMapOf<String, GenerationAndOffset>()

    private var RecoveryTimeCounter: Long = 0

    @Volatile
    private var recoveryComplete = false

    fun receive(name: String, GAO: GenerationAndOffset) {
        if (recoveryComplete) {
            RecoveryMap[name] = GAO

            if (RecoveryMap.size >= ElectMeta.quorum) {
                var newest: MutableMap.MutableEntry<String, GenerationAndOffset>? = null
                RecoveryMap.entries.forEach(Consumer {
                    if (newest == null || it.value > newest!!.value) {
                        newest = it
                    }
                })

                if (newest!!.value == ByteBufPreLogManager.getINSTANCE().commitGAO) {
                    logger.info("已有过半节点提交了最大进度，且集群最大进度 ${newest!!.value} 与 Leader 节点相同，集群已恢复")
                    HanabiListener.onEvent(EventEnum.RECOVERY_COMPLETE)
                } else {
                    logger.info("已有过半节点提交了最大进度，集群最大进度于节点 ${newest!!.key} ，进度为 ${newest!!.value}")
                }
            }
        }
    }

    private fun sendLatestCommitGao() {
        ApisManager.send(ElectMeta.leader!!, RecoveryReporter(ByteBufPreLogManager.getINSTANCE().commitGAO), RequestProcessor.REQUIRE_NESS)
    }
}
