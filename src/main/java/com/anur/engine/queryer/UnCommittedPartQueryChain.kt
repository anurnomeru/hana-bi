package com.anur.engine.queryer

import com.anur.engine.queryer.common.QueryerChain
import com.anur.engine.result.QueryerDefinition
import com.anur.engine.result.common.ResultHandler
import com.anur.engine.storage.memory.MemoryMVCCStorageUnCommittedPartExecutor

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
class UnCommittedPartQueryChain : QueryerChain() {
    override fun doQuery(resultHandler: ResultHandler) {
        val parameterHandler = resultHandler.getParameterHandler()
        MemoryMVCCStorageUnCommittedPartExecutor.queryKeyInTrx(parameterHandler.trxId, parameterHandler.key)
                ?.also {
                    resultHandler.engineResult.hanabiEntry = it
                    resultHandler.engineResult.queryExecutorDefinition = QueryerDefinition.UN_COMMIT_PART
                }
    }
}