package com.anur.engine

import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.memory.MemoryLSM
import com.anur.engine.storage.memory.MemoryMVCCStorageCommittedPart
import com.anur.engine.storage.memory.MemoryMVCCStorageUnCommittedPart
import com.anur.engine.trx.lock.TrxFreeQueuedSynchronizer

/**
 * Created by Anur IjuoKaruKas on 2019/10/31
 *
 * 专用于数据查询
 */
object EngineDataQueryer {

    fun doQuery(trxId: Long, key: String): HanabiEntry? {
        var result: HanabiEntry? = null
        // 如果自己这个事务进行了操作但是没提交，优先取没提交的数据// TODO 存在 UC -> C 过程中数据丢失问题
        if (TrxFreeQueuedSynchronizer.isKeyInUnCommitTrx(trxId, key)) result = MemoryMVCCStorageUnCommittedPart.queryKeyInTrx(trxId, key)

        result = result ?: MemoryMVCCStorageCommittedPart.queryKeyInTrx(trxId, key) ?: MemoryLSM.get(key)

        return if (result?.operateType == HanabiEntry.Companion.OperateType.DISABLE) {
            null
        } else {
            result
        }
    }
}