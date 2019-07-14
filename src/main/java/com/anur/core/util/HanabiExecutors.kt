package com.anur.core.util

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * Created by Anur IjuoKaruKas on 2019/7/14
 */
class HanabiExecutors {

    companion object {
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
            Pool = _HanabiExecutors(threadCount, threadCount, 5, TimeUnit.MILLISECONDS, MissionQueue, ThreadFactoryBuilder().setNameFormat("Hana Pool")
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

    class _HanabiExecutors(corePoolSize: Int, maximumPoolSize: Int, keepAliveTime: Long, unit: TimeUnit?, workQueue: BlockingQueue<Runnable>?, threadFactory: ThreadFactory?) : ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory) {

        override fun afterExecute(r: Runnable?, t: Throwable?) {
            super.afterExecute(r, t)

            var tToLog: Throwable? = t
            if (t == null && r is Future<*>) {
                try {
                    val future = r as Future<*>
                    if (future.isDone) {
                        future.get()
                    }
                } catch (ce: CancellationException) {
                    tToLog = ce
                } catch (ee: ExecutionException) {
                    tToLog = ee.cause
                } catch (ie: InterruptedException) {
                    Thread.currentThread()
                        .interrupt()
                }
            }

            if (tToLog != null) {
                logger.error(tToLog.message, tToLog)
            }
        }
    }
}