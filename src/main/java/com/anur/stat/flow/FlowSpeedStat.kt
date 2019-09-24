package com.anur.stat.flow

import org.slf4j.LoggerFactory


/**
 * Created by Anur IjuoKaruKas on 2019/9/24
 */
object FlowSpeedStat {

    val LogAppend = "Log - Append"
    val PreLogAppend = "PreLog - Append"

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val Clac: MutableMap<String, FlowSpeedStatContainer> = mutableMapOf()

    init {
        create(LogAppend)
        create(PreLogAppend)
    }

    fun incr(key: String, long: Long) {
//        Clac[key]?.incr(long)
    }


    private fun create(key: String) {
//        Clac[key] = FlowSpeedStatContainer(key, logger, 5)
    }
}