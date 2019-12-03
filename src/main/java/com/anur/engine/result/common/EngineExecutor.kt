package com.anur.engine.result.common

import com.anur.core.log.Debugger
import com.anur.core.log.DebuggerLevel
import com.anur.engine.result.EngineResult
import com.anur.engine.storage.core.HanabiEntry
import com.anur.exception.UnexpectedException
import java.lang.StringBuilder


/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 *
 * 辅助访问访问数据层的媒介
 */
class EngineExecutor(val engineResult: EngineResult) {

    companion object {
        val logger = Debugger(EngineExecutor.javaClass).switch(DebuggerLevel.INFO)
    }

    private var parameterHandler: ParameterHandler? = null

    fun hanabiEntry(): HanabiEntry? = engineResult.hanabiEntry

    fun setParameterHandler(parameterHandler: ParameterHandler) {
        this.parameterHandler = parameterHandler
    }

    fun getParameterHandler(): ParameterHandler = parameterHandler ?: throw UnexpectedException("参数没有设置？？？？")

    /**
     * 打印结果
     */
    fun printResult() {
        logger.trace("${engineResult.hanabiEntry}")
    }

    /**
     * 标记为失败
     */
    fun shotFailure() {
        engineResult.result = false
    }

    /**
     * 如果发生了错误
     */
    fun exceptionCaught(e: Throwable) {
        engineResult.err = e
        engineResult.result = false
    }
}