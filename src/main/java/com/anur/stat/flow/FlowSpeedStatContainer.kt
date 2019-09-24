package com.anur.stat.flow

import com.anur.util.TimeUtil
import com.anur.timewheel.TimedTask
import com.anur.timewheel.Timer
import org.slf4j.Logger
import java.util.concurrent.TimeUnit


/**
 * Created by Anur IjuoKaruKas on 2019/9/24
 */
class FlowSpeedStatContainer(private val sign: String, private val logger: Logger, private val interval: Int) {

    private val recordInterval = TimeUnit.SECONDS.toMillis(interval.toLong())

    private val holder: MutableMap<Long, Long> = mutableMapOf()

    init {
        Timer.getInstance().addTask(TimedTask(recordInterval, Runnable { log() }))
    }

    @Synchronized
    fun incr(value: Long) {
        var time = TimeUtil.getTime()
        time -= (time % recordInterval)
        holder[time] = (holder[time] ?: 0L) + value
    }

    fun log() {
        var time = TimeUtil.getTime() - recordInterval
        time -= (time % recordInterval)
        logger.info("$sign 在 ${interval}s 中流动速率为 ${holder[time] ?: 0}")
        Timer.getInstance().addTask(TimedTask(recordInterval, Runnable { log() }))
    }
}