package com.anur.engine

import com.anur.core.lock.rentrant.ReentrantLocker
import com.anur.core.struct.base.Operation
import com.anur.engine.api.Postman
import com.anur.engine.api.common.base.EngineRequest
import com.anur.io.core.coder.CoordinateDecoder
import com.anur.util.HanabiExecutors
import org.slf4j.LoggerFactory
import java.util.concurrent.LinkedBlockingQueue


/**
 * Created by Anur IjuoKaruKas on 2019/10/10
 *
 * 存储引擎唯一对外开放的入口, 这使得存储引擎可以高内聚
 */
object EngineFacade {
    private val logger = LoggerFactory.getLogger(CoordinateDecoder::class.java)
    private val queue = LinkedBlockingQueue<Operation>()
    private val pauseLatch = ReentrantLocker()

    init {
        HanabiExecutors.execute(Runnable {
            logger.info("存储引擎已经启动")
            while (true) {
                val take = queue.poll()

                val hanabiEntry = take.hanabiEntry
                val api = Postman.disPatchType(hanabiEntry.getType()).api(hanabiEntry.getApi())
                api.invoke(EngineRequest(hanabiEntry.getTrxId(), take.key, hanabiEntry.getValue()))
            }
        })
    }

//    fun latestCommit(): GenerationAndOffset {
//
//    }

    /**
     * 继续消费
     */
    fun play() {

    }

    /**
     * 追加消息
     */
    fun append(operation: Operation) {
        queue.put(operation)
    }
}