package com.anur.engine.queryer

import com.anur.engine.queryer.common.QueryerChain
import com.anur.engine.result.QueryerDefinition
import com.anur.engine.result.common.EngineExecutor
import com.anur.engine.storage.memory.MemoryMVCCStorageCommittedPartExecutor

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 *
 *     这个是最特殊的，因为已提交部分的隔离性控制比较复杂
 *             涉及到事务创建时，
 */
class CommittedPartQueryChain : QueryerChain() {
    override fun doQuery(engineExecutor: EngineExecutor) {
        val parameterHandler = engineExecutor.getParameterHandler()
        MemoryMVCCStorageCommittedPartExecutor.queryKeyInTrx(parameterHandler.trxId, parameterHandler.key, parameterHandler.getWaterMarker())
                ?.also {
                    engineExecutor.engineResult.setHanabiEntry(it)
                    engineExecutor.engineResult.queryExecutorDefinition = QueryerDefinition.COMMIT_PART
                }
    }
}