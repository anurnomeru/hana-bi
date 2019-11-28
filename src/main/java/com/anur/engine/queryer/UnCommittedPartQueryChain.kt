package com.anur.engine.queryer

import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.memory.MemoryMVCCStorageUnCommittedPart
import com.anur.engine.trx.watermark.WaterMarker

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
class UnCommittedPartQueryChain : QueryerChain() {
    override fun doQuery(trxId: Long, key: String, waterMarker: WaterMarker): HanabiEntry? = MemoryMVCCStorageUnCommittedPart.queryKeyInTrx(trxId, key)
}