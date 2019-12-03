package com.anur.engine.queryer

import com.anur.engine.queryer.common.QueryParameterHandler
import com.anur.engine.queryer.common.QueryerChain
import com.anur.engine.result.QueryerDefinition
import com.anur.engine.result.common.ResultHandler
import com.anur.engine.storage.memory.MemoryLSMExecutor

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
class MemoryLSMQueryChain : QueryerChain() {
    override fun doQuery(parameterHandler: QueryParameterHandler, resultHandler: ResultHandler) {
        MemoryLSMExecutor.get(parameterHandler.key)
                ?.also {
                    resultHandler.engineResult.hanabiEntry = it
                    resultHandler.engineResult.queryExecutorDefinition = QueryerDefinition.MEMORY_LSM
                }
    }

}