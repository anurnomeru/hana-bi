package com.anur.engine.trx.manager

import com.anur.engine.trx.watermark.WaterHolder
import kotlin.math.max

/**
 * 为了避免事务太多，导致列表太大，故采用分段，类似于位图的思想
 */
class TrxSegment(anyElse: Long) {

    /**
     * 标记此事务第一位的事务id，比如为 100
     * 那么 trxBitMap 0011 代表 100 101 事务已经激活
     */
    val start: Long = WaterHolder.genSegmentHead(anyElse) * WaterHolder.Interval

    /**
     * 使用 bitMap 来实现事务的激活与释放，如果一个事务被激活了，则此位标记为1，反之为0
     */
    @Volatile
    var trxBitMap: Long = 0

    /**
     * 使用 bitMap 来实现事务的释放，如果一个事务被释放了，则此标记为为1，反之为0
     */
    @Volatile
    var releaseBitMap: Long = 0

    /**
     * 拷贝一份，快照用
     */
    fun copyOf(): TrxSegment {
        val neo = TrxSegment(start)
        neo.trxBitMap = trxBitMap
        neo.releaseBitMap = releaseBitMap
        return neo
    }

    fun minTrx(): Long? {
        return if (releaseBitMap == 0L) {
            null
        } else {
            var result = 0
            var mask = 1L
            while (result < WaterHolder.Interval) {
                if (mask and releaseBitMap != mask) {
                    break
                }
                result++
                mask = mask shl 1
            }
            start + max(result - 1, 0)
        }
    }

    /**
     * 激活某个事务，其实就是把它置为1
     *
     * 返回 false 表示以前被激活过
     * 返回 true 表示第一次激活
     */
    fun activate(trxId: Long): Boolean {
        val index = calcIndex(trxId)
        val mask = 1L.shl(index)

        val firstActive = mask and trxBitMap == 0L
        trxBitMap = trxBitMap or mask
        return firstActive
    }

    /**
     * 查询某个事务是否处于激活状态
     */
    fun isActivate(trxId: Long): Boolean {
        val index = calcIndex(trxId)
        val mask = 1L.shl(index)

        return mask and trxBitMap != 0L
    }

    /**
     * 标记某个事务已经释放
     */
    fun release(trxId: Long): Int {
        val index = calcIndex(trxId)
        val mask = 1L.shl(index)
        releaseBitMap = trxBitMap or mask
        return index
    }

    private fun calcIndex(trxId: Long): Int {
        return ((WaterHolder.Interval - 1) and trxId).toInt()
    }
}