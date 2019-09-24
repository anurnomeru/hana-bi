package com.anur.stat.flow

import org.slf4j.LoggerFactory


/**
 * Created by Anur IjuoKaruKas on 2019/9/24
 */
object FlowSpeedStat {

    val LogAppend = "Log - Append"

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val Clac: MutableMap<String, FlowSpeedStatContainer> = mutableMapOf()

    init {
        Clac[LogAppend] = FlowSpeedStatContainer(10, logger) { "【日志】近十秒，本节点日志刷盘的数据速率为 $it" }
    }

    fun incr(key: String, long: Long) {
        Clac[key]?.incr(long)
    }

}