package com.anur.core.coordinate.apis.fetch

import com.anur.config.CoordinateConfigHelper
import com.anur.core.coordinate.apis.driver.ApisManager
import com.anur.core.coordinate.model.RequestProcessor
import com.anur.core.elect.ElectMeta
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
 * Created by Anur IjuoKaruKas on 2019/7/27
 */
abstract class CoordinateFetcher : ReentrantReadWriteLocker() {

    private val logger = LoggerFactory.getLogger(this::class.java)

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

    protected fun fetchLocker(doSomething: () -> Unit) {
        fetchLock.lock()
        try {
            doSomething.invoke()
        } finally {
            fetchLock.unlock()
        }
    }

    protected fun cancelFetchTask() {
        cvc++
        fetchPreLogTask?.cancel()
        logger.info("取消 FetchPreLog 定时任务")
    }


    /**
     * 构建开始从 Leader 同步操作日志的定时任务
     */
    protected fun startToFetchFromLeader() {
        if (!ElectMeta.isLeader) {
            FollowerCoordinateManager.writeLocker {
                // 如果节点非Leader，需要连接 Leader，并创建 Fetch 定时任务
                FollowerCoordinateManager.fetchLocker { FollowerCoordinateManager.rebuildFetchTask(cvc, ElectMeta.leader!!) }
            }
        }
    }

    /**
     * 构建开始从 某节点 同步操作日志的定时任务
     */
    protected fun startToFetchFrom(serverName: String) {
        FollowerCoordinateManager.writeLocker {
            // 如果节点非Leader，需要连接 Leader，并创建 Fetch 定时任务
            FollowerCoordinateManager.fetchLocker { FollowerCoordinateManager.rebuildFetchTask(cvc, serverName) }
        }
    }

    /**
     * 重构从某节点获取操作日志的定时任务
     *
     * 实际上就是不断创建定时任务，扔进时间轮，任务的内容则是调用 sendFetchPreLog 方法
     */
    protected fun rebuildFetchTask(myVersion: Long, fetchFrom: String) {
        if (cvc > myVersion) {
            logger.debug("sendFetchPreLog Task is out of version.")
            return
        }

        fetchPreLogTask = TimedTask(CoordinateConfigHelper.getFetchBackOfMs().toLong()) { sendFetchPreLog(myVersion, fetchFrom) }
        Timer.getInstance()
            .addTask(fetchPreLogTask)
    }


    /**
     * 主要负责定时 Fetch 消息
     *
     * 新建一个 Fetcher 用于拉取消息，将其发送给 Leader，并在收到回调后，调用 CONSUME_FETCH_RESPONSE 消费回调，且重启拉取定时任务
     */
    private fun sendFetchPreLog(myVersion: Long, fetchFrom: String) {
        fetchLock.lock()
        try {
            fetchPreLogTask?.takeIf { !it.isCancel }?.run {
                ApisManager.send(fetchFrom,
                    Fetcher(ByteBufPreLogManager.getPreLogGAO()),
                    RequestProcessor(Consumer { readLocker { howToConsumeFetchResponse(fetchFrom, FetchResponse(it)) } },
                        Runnable { rebuildFetchTask(myVersion, fetchFrom) })
                )
            }
        } finally {
            fetchLock.unlock()
        }
    }


    /**
     * 子类定义如何消费Response
     */
    abstract fun howToConsumeFetchResponse(fetchFrom: String, fetchResponse: FetchResponse)
}