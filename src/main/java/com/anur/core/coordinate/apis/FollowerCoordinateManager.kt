package com.anur.core.coordinate.apis

import com.anur.config.CoordinateConfigHelper
import com.anur.config.InetSocketAddressConfigHelper
import com.anur.core.coordinate.apis.driver.ApisManager
import com.anur.core.coordinate.model.RequestProcessor
import com.anur.core.coordinate.operator.CoordinateClientOperator
import com.anur.core.elect.ElectMeta
import com.anur.core.listener.EventEnum
import com.anur.core.listener.HanabiListener
import com.anur.core.lock.ReentrantReadWriteLocker
import com.anur.core.struct.coordinate.FetchResponse
import com.anur.core.struct.coordinate.Fetcher
import com.anur.io.store.prelog.ByteBufPreLogManager
import com.anur.timewheel.TimedTask
import com.anur.timewheel.Timer
import org.slf4j.LoggerFactory
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Consumer

/**
 * Created by Anur IjuoKaruKas on 2019/7/9
 *
 * 日志一致性控制器 Follower 端
 */
object FollowerCoordinateManager : ReentrantReadWriteLocker() {

    private val logger = LoggerFactory.getLogger(this.javaClass)

    init {
        HanabiListener.register(EventEnum.RECOVERY_COMPLETE) {
            if (!ElectMeta.isLeader) {
                writeLocker {
                    // 如果节点非Leader，需要连接 Leader，并创建 Fetch 定时任务
                    fetchLocker { rebuildFetchTask(cvc, ElectMeta.leader!!) }
                }
            }
        }

        HanabiListener.register(EventEnum.COORDINATE_DISCONNECT_TO_LEADER) {
            cancelFetchTask()
            cvc++
        }

        HanabiListener.register(EventEnum.CLUSTER_INVALID
        ) {
            writeLocker {

                cvc++
                cancelFetchTask()

                // 当集群不可用时，与协调 leader 断开连接
                CoordinateClientOperator.shutDownInstance("集群已不可用，与协调 Leader 断开连接")
            }
        }
    }


    /**
     * 此字段用作版本控制，定时任务仅执行小于等于自己版本的任务
     *
     * Coordinate Version Control
     */
    @Volatile
    private var cvc: Long = 0

    /**
     * 作为 Follower 时有效，此任务不断从 Leader 节点获取 PreLog
     */
    private var fetchPreLogTask: TimedTask? = null

    /**
     * Fetch 锁
     */
    private val fetchLock = ReentrantLock()

    private fun fetchLocker(doSomething: () -> Unit) {
        fetchLock.lock()
        try {
            doSomething.invoke()
        } finally {
            fetchLock.unlock()
        }
    }

    private fun cancelFetchTask() {
        fetchPreLogTask?.cancel()
        logger.info("取消 FetchPreLog 定时任务")
    }

    private fun rebuildFetchTask(myVersion: Long, fetchFrom: String) {
        if (cvc > myVersion) {
            logger.debug("sendFetchPreLog Task is out of version.")
            return
        }

        fetchPreLogTask = TimedTask(CoordinateConfigHelper.getFetchBackOfMs().toLong()) { sendFetchPreLog(myVersion, fetchFrom) }
        Timer.getInstance()
            .addTask(fetchPreLogTask)
    }

    /**
     * 主要负责定时 Fetch 消息，这部分消息称为 preLog，
     */
    private fun sendFetchPreLog(myVersion: Long, fetchFrom: String) {
        fetchLock.lock()
        try {
            fetchPreLogTask?.takeIf { !it.isCancel }?.run {
                ApisManager.send(fetchFrom,
                    Fetcher(ByteBufPreLogManager.getPreLogGAO()),
                    RequestProcessor(Consumer { CONSUME_FETCH_RESPONSE.invoke(fetchFrom, FetchResponse(it)) }, Runnable { rebuildFetchTask(myVersion, fetchFrom) })
                )
            }
        } finally {
            fetchLock.unlock()
        }
    }

    /**
     * 如何消费 Fetch response
     */
    private val CONSUME_FETCH_RESPONSE: (String, FetchResponse) -> Unit = { node, fetchResponse ->
        readLocker {
            logger.debug("收到节点 {} 返回的 FETCH_RESPONSE", node)
            ElectMeta.takeIf { it.isLeader }?.run { logger.error("出现了不应该出现的情况！") }

            if (fetchResponse.fileOperationSetSize == 0) return@readLocker
            ByteBufPreLogManager
                .append(fetchResponse.generation, fetchResponse.read())
        }
    }
}
