package com.anur.engine.result.common

import com.anur.engine.result.EngineResult
import com.anur.engine.storage.core.HanabiEntry


/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 */
class ResultHandler(val engineResult: EngineResult) {
    fun hanabiEntry(): HanabiEntry? = engineResult.hanabiEntry

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