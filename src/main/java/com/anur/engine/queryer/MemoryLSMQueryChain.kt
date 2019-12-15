package com.anur.engine.queryer

import com.anur.engine.queryer.common.QueryerChain
import com.anur.engine.result.QueryerDefinition
import com.anur.engine.processor.EngineExecutor
import com.anur.engine.memory.MemoryLSM

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
class MemoryLSMQueryChain : QueryerChain() {
    override fun doQuery(engineExecutor: EngineExecutor) {
        val dataHandler = engineExecutor.getDataHandler()
        MemoryLSM.get(dataHandler.key)
                ?.also {
                    engineExecutor.engineResult.setHanabiEntry(it)
                    engineExecutor.engineResult.queryExecutorDefinition = QueryerDefinition.MEMORY_LSM
                }
    }

}