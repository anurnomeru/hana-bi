package com.anur.core.coordinate.apis.driver

import com.anur.core.coordinate.model.CoordinateRequest
import com.anur.core.util.HanabiExecutors
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2019/8/7
 */
object RequestHandlePool {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val poolSize = 8

    private val handlers = mutableListOf<RequestHandler>()

    init {
        for (i in 0 until poolSize) {
            RequestHandler(i).let { rh ->
                Thread(rh).let {
                    it.name = "RequestHandlePool - $i"
                    it.isDaemon = true
                    HanabiExecutors.execute(it)
                }
                handlers.add(rh)
            }
        }
        logger.info("初始化请求池成功，共有 $poolSize 个请求池被创建")
    }

    fun receiveRequest(request: CoordinateRequest) {
        RequestChannel.receiveRequest(request)
    }

    fun shutDown() {
        for (handler in handlers) {
            handler.shutDown()
        }
        logger.info("关闭请求池，共有 $poolSize 个请求池被关闭")
    }

    fun clear() {
        logger.info("清空请求池内容")
        RequestChannel.requestQueue.clear()
    }
}