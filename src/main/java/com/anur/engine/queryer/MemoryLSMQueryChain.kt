package com.anur.engine.queryer

import com.anur.engine.queryer.common.QueryerChain
import com.anur.engine.result.QueryerDefinition
import com.anur.engine.result.common.EngineExecutor
import com.anur.engine.storage.memory.MemoryLSMExecutor

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
class MemoryLSMQueryChain : QueryerChain() {
    override fun doQuery(engineExecutor: EngineExecutor) {
        val parameterHandler = engineExecutor.getParameterHandler()
        MemoryLSMExecutor.get(parameterHandler.key)
                ?.also {
                    engineExecutor.engineResult.hanabiEntry = it
                    engineExecutor.engineResult.queryExecutorDefinition = QueryerDefinition.MEMORY_LSM
                }
    }

}