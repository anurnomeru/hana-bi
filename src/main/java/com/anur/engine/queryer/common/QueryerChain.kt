package com.anur.engine.queryer.common

import com.anur.engine.result.QueryerDefinition
import com.anur.engine.result.common.EngineExecutor


/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 由于查询功能是由各个小模块提供的，所以使用责任链来实现
 */
abstract class QueryerChain {

    var next: QueryerChain? = null

    /**
     * 本层如何去执行查询
     */
    abstract fun doQuery(engineExecutor: EngineExecutor)

    /**
     * 如果到了最后一层都找不到，则返回此结果
     */
    private fun keyNotFoundTilEnd(engineExecutor: EngineExecutor) {
        engineExecutor.engineResult.queryExecutorDefinition = QueryerDefinition.TIL_END
    }

    fun query(engineExecutor: EngineExecutor) {
        doQuery(engineExecutor).let { engineExecutor.engineResult.getHanabiEntry() }
                ?: next?.query(engineExecutor).let { engineExecutor.engineResult.getHanabiEntry() }
                ?: keyNotFoundTilEnd(engineExecutor)
    }
}

