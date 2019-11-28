package com.anur.engine.queryer

import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.memory.MemoryMVCCStorageCommittedPart
import com.anur.engine.trx.watermark.WaterMarker

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 *
 *     这个是最特殊的，因为已提交部分的隔离性控制比较复杂
 *             涉及到事务创建时，
 */
class CommittedPartQueryChain : QueryerChain() {

    override fun doQuery(trxId: Long, key: String, waterMarker: WaterMarker): HanabiEntry? = MemoryMVCCStorageCommittedPart.queryKeyInTrx(trxId, key, waterMarker)
}