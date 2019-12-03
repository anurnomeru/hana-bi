package com.anur.engine.queryer

import com.anur.engine.result.common.EngineExecutor

/**
 * Created by Anur IjuoKaruKas on 2019/10/31
 *
 * 专用于数据查询
 */
object EngineDataQueryer {

    private val firstChain = UnCommittedPartQueryChain()

    init {
        val cqc = CommittedPartQueryChain()
        firstChain.next = cqc

        val lsmqc = MemoryLSMQueryChain()
        cqc.next = lsmqc
    }

    fun doQuery(engineExecutor: EngineExecutor) = firstChain.query(engineExecutor)
}