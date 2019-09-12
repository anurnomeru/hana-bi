package com.anur.core.util

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.TimeUnit

/**
 * Created by Anur IjuoKaruKas on 2019/7/14
 */
object HanabiExecutors {

    private val logger = LoggerFactory.getLogger(this::class.java)

    /**
     * 线程池的 Queue
     */
    private var MissionQueue = LinkedBlockingDeque<Runnable>()

    /**
     * 统一管理的线程池
     */
    private var Pool: ExecutorService

    init {
        val coreCount = Runtime.getRuntime()
            .availableProcessors()
        val threadCount = coreCount * 2
        logger.info("创建 Hanabi 线程池 => 机器核心数为 {}, 故创建线程 {} 个", coreCount, threadCount)
        Pool = _HanabiExecutors(logger, threadCount, threadCount, 5, TimeUnit.MILLISECONDS, MissionQueue, ThreadFactoryBuilder().setNameFormat("Hana Pool")
            .setDaemon(true)
            .build())
    }

    fun execute(runnable: Runnable) {
        Pool.execute(runnable)
    }

    fun <T> submit(task: Callable<T>): Future<T> {
        return Pool.submit(task)
    }

    fun getBlockSize(): Int {
        return MissionQueue.size
    }
}