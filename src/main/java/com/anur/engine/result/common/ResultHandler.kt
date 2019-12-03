package com.anur.engine.result.common

import com.anur.core.log.Debugger
import com.anur.engine.result.EngineResult
import com.anur.engine.storage.core.HanabiEntry
import com.anur.exception.UnexpectedException


/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 */
class ResultHandler(val engineResult: EngineResult) {

    companion object {
        val logger = Debugger(ResultHandler.javaClass)
    }

    private var parameterHandler: ParameterHandler? = null

    fun hanabiEntry(): HanabiEntry? = engineResult.hanabiEntry

    fun setParameterHandler(parameterHandler: ParameterHandler) {
        this.parameterHandler = parameterHandler
    }

    fun getParameterHandler(): ParameterHandler = parameterHandler ?: throw UnexpectedException("参数没有设置？？？？")

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

    /**
     * 打印结果
     */
    fun printResult() {

    }
}