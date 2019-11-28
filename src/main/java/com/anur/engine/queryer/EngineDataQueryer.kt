package com.anur.engine.queryer

import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.trx.watermark.WaterMarker

/**
 * Created by Anur IjuoKaruKas on 2019/10/31
 *
 * 专用于数据查询
 */
object EngineDataQueryer {

    private val chain = UnCommittedPartQueryChain()

    init {
        val cqc = CommittedPartQueryChain()
        val lsmqc = MemoryLSMQueryChain()
        cqc.next = lsmqc
        chain.next = cqc
    }

    fun doQuery(trxId: Long, key: String, waterMarker: WaterMarker): HanabiEntry? = chain.query(trxId, key, waterMarker)?.takeIf { it.operateType != HanabiEntry.Companion.OperateType.DISABLE }
}