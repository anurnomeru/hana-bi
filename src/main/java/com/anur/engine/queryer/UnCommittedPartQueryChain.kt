package com.anur.engine.queryer

import com.anur.engine.queryer.common.QueryerChain
import com.anur.engine.result.QueryerDefinition
import com.anur.engine.result.common.EngineExecutor
import com.anur.engine.storage.memory.MemoryMVCCStorageUnCommittedPartExecutor

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
class UnCommittedPartQueryChain : QueryerChain() {
    override fun doQuery(engineExecutor: EngineExecutor) {
        val parameterHandler = engineExecutor.getParameterHandler()
        MemoryMVCCStorageUnCommittedPartExecutor.queryKeyInTrx(parameterHandler.trxId, parameterHandler.key)
                ?.also {
                    engineExecutor.engineResult.setHanabiEntry(it)
                    engineExecutor.engineResult.queryExecutorDefinition = QueryerDefinition.UN_COMMIT_PART
                }
    }
}