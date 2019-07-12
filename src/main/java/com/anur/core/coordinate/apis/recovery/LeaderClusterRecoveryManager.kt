package com.anur.core.coordinate.apis.recovery

import com.anur.core.common.Resetable
import com.anur.core.coordinate.apis.driver.ApisManager
import com.anur.core.coordinate.model.RequestProcessor
import com.anur.core.elect.ElectMeta
import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.listener.EventEnum
import com.anur.core.listener.HanabiListener
import com.anur.core.struct.coordinate.RecoveryComplete
import com.anur.core.util.TimeUtil
import com.anur.io.store.prelog.ByteBufPreLogManager
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
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
object LeaderClusterRecoveryManager : Resetable {

    override fun reset() {
        logger.debug("LeaderClusterRecoveryManager RESET is triggered")
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
            reset()
            RecoveryTimer = TimeUtil.getTime()
            if (ElectMeta.isLeader) {
                receive(ElectMeta.leader!!, ByteBufPreLogManager.getINSTANCE().commitGAO)
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
    private var waitShutting = ConcurrentSkipListSet<String>()

    @Volatile
    private var RecoveryMap = ConcurrentHashMap<String, GenerationAndOffset>()

    @Volatile
    private var recoveryComplete = false

    fun receive(serverName: String, latestGao: GenerationAndOffset) {
        logger.info("节点 $serverName 提交了其最大进度 $latestGao ")
        RecoveryMap[serverName] = latestGao

        if (!recoveryComplete) {
            if (RecoveryMap.size >= ElectMeta.quorum) {
                var newest: MutableMap.MutableEntry<String, GenerationAndOffset>? = null
                RecoveryMap.entries.forEach(Consumer {
                    if (newest == null || it.value > newest!!.value) {
                        newest = it
                    }
                })

                if (newest!!.value == ByteBufPreLogManager.getINSTANCE().commitGAO) {
                    val cost = TimeUtil.getTime() - RecoveryTimer
                    logger.info("已有过半节点提交了最大进度，且集群最大进度 ${newest!!.value} 与 Leader 节点相同，集群已恢复，耗时 $cost ms ")
                    shuttingWhileRecoveryComplete()
                } else {
                    logger.info("已有过半节点提交了最大进度，集群最大进度于节点 ${newest!!.key} ，进度为 ${newest!!.value}")
                }
            }
        }

        sendRecoveryComplete(serverName)
    }

    private fun shuttingWhileRecoveryComplete() {
        recoveryComplete = true
        waitShutting.forEach(Consumer { sendRecoveryComplete(it) })
    }

    private fun sendRecoveryComplete(serverName: String) {
        if (recoveryComplete) {
            doSendRecoveryComplete(serverName)
        } else {
            waitShutting.add(serverName)
        }
    }

    private fun doSendRecoveryComplete(serverName: String) {
        ApisManager.send(serverName, RecoveryComplete(), RequestProcessor.REQUIRE_NESS)
    }
}