package com.anur.engine.queryer

import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.memory.MemoryMVCCStorageCommittedPart

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
class CommittedPartQueryChain : QueryerChain() {
    override fun doQuery(trxId: Long, key: String): HanabiEntry? = MemoryMVCCStorageCommittedPart.queryKeyInTrx(trxId, key)
}