package com.anur.core.coordinate.apis.driver

import com.anur.core.coordinate.model.CoordinateRequest
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * Created by Anur IjuoKaruKas on 2019/8/7
 *
 * 此线程用于消费受到的消息
 */
class RequestHandler(private val id: Int) : Runnable {

    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun run() {
        while (true) {
            val request = RequestChannel.consumeRequest(300, TimeUnit.MILLISECONDS).takeIf { it!!.channel != null } ?: continue
            if (request == CoordinateRequest.AllDone) {
                logger.info("RequestHandler $id receive shutDown.")
            }
            ApisManager.receive(request.msg, request.typeEnum, request.channel!!)
        }
    }

    fun shutDown() {
        RequestChannel.receiveRequest(CoordinateRequest.AllDone)
    }
}