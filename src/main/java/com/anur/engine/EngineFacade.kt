package com.anur.engine

import com.anur.core.elect.model.GenerationAndOffset
import com.anur.engine.api.Postman
import com.anur.engine.api.common.base.EngineRequest
import com.anur.io.core.coder.CoordinateDecoder
import com.anur.io.hanalog.common.OperationAndGAO
import com.anur.io.hanalog.log.CommitProcessManager
import com.anur.util.HanabiExecutors
import org.slf4j.LoggerFactory
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock


/**
 * Created by Anur IjuoKaruKas on 2019/10/10
 *
 * 存储引擎唯一对外开放的入口, 这使得存储引擎可以高内聚
 */
object EngineFacade {
    private val logger = LoggerFactory.getLogger(CoordinateDecoder::class.java)
    private val queue = LinkedBlockingQueue<OperationAndGAO>()
    private val lock = ReentrantLock()
    private val pauseLatch = lock.newCondition()

    init {
        HanabiExecutors.execute(Runnable {
            // 启动锁
            lock.lock()
            pauseLatch.await()
            lock.unlock()

            logger.error("存储引擎已经启动!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            while (true) {
                val take = queue.take()

                try {
                    blockCheckIter(take.GAO)
                } catch (e: Exception) {
                    println()
                }

                val hanabiEntry = take.operation.hanabiCommand
                val api = Postman.disPatchType(hanabiEntry.getType()).api(hanabiEntry.getApi())
                api.invoke(EngineRequest(hanabiEntry.getTrxId(), take.operation.key, hanabiEntry.getValue()))
            }
        })
    }

    private val counter = AtomicInteger()

    /**
     * 检查是否需要阻塞
     */
    private fun blockCheckIter(Gao: GenerationAndOffset) {
        val latestCommitted = CommitProcessManager.load()
        if (latestCommitted != GenerationAndOffset.INVALID && Gao > latestCommitted) {
            lock.lock()
            logger.error("存储引擎已经消费到最新的commit进度${latestCommitted}，故暂停!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            pauseLatch.await()
            logger.error("存储引擎被唤醒!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            lock.unlock()
            blockCheckIter(Gao)
        } else {
            if (counter.incrementAndGet() % 100000 == 0) logger.info("当前最新提交： ${latestCommitted}，消费进度：${Gao} ")
        }
    }

    /**
     * 继续消费
     */
    fun play(Gao: GenerationAndOffset) {
        lock.lock()
        pauseLatch.signalAll()
        CommitProcessManager.cover(Gao)
        pauseLatch.signalAll()
        lock.unlock()
    }

    /**
     * 追加消息
     */
    fun append(oaGao: OperationAndGAO) {
        queue.put(oaGao)
    }
}
