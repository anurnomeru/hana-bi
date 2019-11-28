package com.anur.engine.queryer

import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.memory.MemoryLSM
import com.anur.engine.storage.memory.MemoryMVCCStorageCommittedPart
import com.anur.engine.trx.watermark.WaterMarker

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
class MemoryLSMQueryChain : QueryerChain() {
    override fun doQuery(trxId: Long, key: String, waterMarker: WaterMarker): HanabiEntry? = MemoryLSM.get(key)
}