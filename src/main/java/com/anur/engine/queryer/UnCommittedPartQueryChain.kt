package com.anur.engine.queryer

import com.anur.engine.queryer.common.QueryerChain
import com.anur.engine.processor.QueryerDefinition
import com.anur.engine.processor.common.EngineExecutor
import com.anur.engine.memory.MemoryMVCCStorageUnCommittedPart

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
class UnCommittedPartQueryChain : QueryerChain() {
    override fun doQuery(engineExecutor: EngineExecutor) {
        val dataHandler = engineExecutor.getDataHandler()
        MemoryMVCCStorageUnCommittedPart.queryKeyInTrx(dataHandler.getTrxId(), dataHandler.key)
                ?.also {
                    engineExecutor.engineResult.setHanabiEntry(it)
                    engineExecutor.engineResult.queryExecutorDefinition = QueryerDefinition.UN_COMMIT_PART
                }
    }
}