package com.anur.engine.result.common

import com.anur.core.log.Debugger
import com.anur.core.log.DebuggerLevel
import com.anur.engine.result.EngineResult
import com.anur.engine.storage.entry.ByteBufferHanabiEntry
import com.anur.exception.UnexpectedException


/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 *
 * 辅助访问访问数据层的媒介
 */
class EngineExecutor(val engineResult: EngineResult) {

    companion object {
        val logger = Debugger(EngineExecutor.javaClass).switch(DebuggerLevel.INFO)
    }

    private var dataHandler: DataHandler? = null

    fun hanabiEntry(): ByteBufferHanabiEntry? = engineResult.getHanabiEntry()

    fun setDataHandler(dataHandler: DataHandler) {
        this.dataHandler = dataHandler
    }

    fun getDataHandler(): DataHandler = dataHandler ?: throw UnexpectedException("参数没有设置？？？？")

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