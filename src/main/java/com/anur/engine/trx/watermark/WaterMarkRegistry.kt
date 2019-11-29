package com.anur.engine.trx.watermark

/**
 * Created by Anur IjuoKaruKas on 2019/11/28
 *
 * 存放水位快照的地方，没别的用途
 *
 * TODO 超时销毁机制，用这个类来控制比如事务超时等等
 */
object WaterMarkRegistry {
    private val registry = mutableMapOf<Long, WaterMarker>()

    fun register(trxId: Long, waterMarker: WaterMarker) {
        registry[trxId] = waterMarker
    }

    fun release(trxId: Long) {
        registry.remove(trxId)
    }

    fun findOut(trxId: Long): WaterMarker {
        val waterMarker = registry[trxId]
        return waterMarker ?: WaterMarker.NONE
    }
}