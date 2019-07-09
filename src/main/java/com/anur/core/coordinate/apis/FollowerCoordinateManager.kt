package com.anur.core.coordinate.apis

import com.anur.config.CoordinateConfigHelper
import com.anur.config.InetSocketAddressConfigHelper
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
import java.util.Optional
import java.util.concurrent.locks.ReentrantLock

/**
 * Created by Anur IjuoKaruKas on 2019/7/9
 *
 * 日志一致性控制器 Follower 端
 */
object FollowerCoordinateManager: ReentrantReadWriteLocker() {

    private val logger = LoggerFactory.getLogger(CoordinateApisManager::class.java)

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

    init {
        HanabiListener.register(EventEnum.CLUSTER_VALID
        ) {
            if (!ElectMeta.isLeader) {
                writeLockSupplier<Any?> {
                    val client = CoordinateClientOperator.getInstance(InetSocketAddressConfigHelper.getNode(ElectMeta.leader))

                    // 如果节点非Leader，需要连接 Leader，并创建 Fetch 定时任务
                    // 当集群可用时，连接协调 leader

                    client.registerWhenConnectToLeader {
                        fetchLock.lock()
                        try {
                            rebuildFetchTask(cvc, ElectMeta.leader)
                        } finally {
                            fetchLock.unlock()
                        }
                    }

                    client.registerWhenDisconnectToLeader {
                        fetchLock.lock()
                        try {
                            cancelFetchTask()
                            cvc++
                        } finally {
                            fetchLock.unlock()
                        }
                    }
                    client.tryStartWhileDisconnected()
                    null
                }
            }
            null
        }

        HanabiListener.register(EventEnum.CLUSTER_INVALID
        ) {
            writeLockSupplier {
                ApisManager.getINSTANCE()
                    .reboot()

                cvc++
                cancelFetchTask()

                // 当集群不可用时，与协调 leader 断开连接
                CoordinateClientOperator.shutDownInstance("集群已不可用，与协调 Leader 断开连接")
                null
            }
        }
    }

    private fun cancelFetchTask() {
        fetchPreLogTask?.cancel()
        logger.debug("取消 FetchPreLog 定时任务")
    }

    private fun rebuildFetchTask(myVersion: Long, fetchFrom: String?) {
        if (cvc > myVersion) {
            logger.debug("sendFetchPreLog Task is out of version.")
            return
        }

        fetchPreLogTask = TimedTask(CoordinateConfigHelper.getFetchBackOfMs().toLong()) { sendFetchPreLog(myVersion, fetchFrom) }
        Timer.getInstance()
            .addTask(fetchPreLogTask)
        logger.debug("载入 FetchPreLog 定时任务")
    }

    /**
     * 定时 Fetch 消息
     */
    fun sendFetchPreLog(myVersion: Long, fetchFrom: String?) {
        fetchLock.lock()
        try {
            fetchPreLogTask?.takeIf { it.isCancel }.run {

            }

            Optional.ofNullable<Any>(fetchPreLogTask)
                .filter { fetchTask -> !fetchTask.isCancel() }
                .ifPresent { fetchTask ->
                    if (ApisManager.getINSTANCE()
                            .send(
                                fetchFrom,
                                Fetcher(
                                    ByteBufPreLogManager.getINSTANCE()
                                        .preLogGAO
                                ),
                                RequestProcessor({ byteBuffer -> CONSUME_FETCH_RESPONSE.accept(fetchFrom, FetchResponse(byteBuffer)) },
                                    { rebuildFetchTask(myVersion, fetchFrom) }))) {
                    }
                }
        } finally {
            fetchLock.unlock()
        }
    }

    /**
     * 如何消费 Fetch response
     */
    private val CONSUME_FETCH_RESPONSE = { node, fetchResponse ->
        readLockSupplier<Any> {
            logger.debug("收到节点 {} 返回的 FETCH_RESPONSE", node)

            if (ElectMeta.isLeader) {
                logger.error("出现了不应该出现的情况！")
            }

            if (fetchResponse.getFileOperationSetSize() == 0) {
                return@readLockSupplier null
            }

            ByteBufPreLogManager.getINSTANCE()
                .append(fetchResponse.getGeneration(), fetchResponse.read())
            null
        }
    }
}