package com.anur.core.coordinate.apis.driver

import com.anur.core.coordinate.model.CoordinateRequest
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Created by Anur IjuoKaruKas on 2019/8/7
 */
object RequestChannel {
    val requestQueue: ArrayBlockingQueue<CoordinateRequest> = ArrayBlockingQueue(Runtime.getRuntime().availableProcessors() * 2)

    fun receiveRequest(request: CoordinateRequest) {
        requestQueue.put(request)
    }

    fun consumeRequest(timeout: Long, unit: TimeUnit): CoordinateRequest? {
        return requestQueue.poll(timeout, unit)
    }
}