package com.anur.engine.trx.watermark

import com.anur.core.log.Debugger
import com.anur.engine.trx.manager.TrxAllocator
import com.anur.engine.trx.manager.TrxSegment
import java.util.*
import javax.xml.bind.annotation.XmlType
import kotlin.Comparator
import kotlin.math.absoluteValue


/**
 * Created by Anur IjuoKaruKas on 2019/11/28
 *
 * 控制一系列区间的水位
 */
class WaterHolder {

    companion object {
        const val Interval = 64L
        private const val IntervalMinusOne = 63
        private const val IntervalMinusMask = 1L.shl(IntervalMinusOne)
        private val logger = Debugger(WaterHolder.javaClass)
        val DEFAULT = WaterHolder()

        /**
         * 算出每一个段的“段头”
         */
        fun genSegmentHead(trxId: Long): Long {
            return if (trxId < 0) {
                -((trxId + 1).absoluteValue / Interval + 1)
            } else {
                trxId / Interval
            }
        }
    }

    private val waterHolder = TreeMap<Long, TrxSegment>(Comparator { o1, o2 -> o1.compareTo(o2) })

    /**
     * 为每个事务创建一个未提交的事务的水位快照
     */
    fun snapshot(): WaterHolder {
        val snapshot = WaterHolder()
        for (mutableEntry in waterHolder) {
            snapshot.waterHolder[mutableEntry.key] = mutableEntry.value.copyOf()
        }
        return snapshot
    }

    /**
     * 判断一个事务是否活跃
     */
    fun isActivateTrx(trxId: Long): Boolean {
        val head = genSegmentHead(trxId)
        return waterHolder[head]?.isActivate(trxId) ?: false
    }

    /**
     * 从map中拿到此段，再去激活此段中的此事务id
     *
     * 返回 false 表示以前被激活过
     * 返回 true 表示第一次激活
     */
    fun activateTrx(TrxId: Long): Boolean {
        val head = genSegmentHead(TrxId)

        // 将事务扔进水位
        if (!waterHolder.contains(head)) waterHolder[head] = TrxSegment(TrxId)
        return waterHolder[head]!!.activate(TrxId)
    }

    /**
     * 从map中拿到此段，再去释放此段中的此事务id
     */
    fun releaseTrx(TrxId: Long): WaterReleaseResult {
        val head = genSegmentHead(TrxId)

        when (val trxSegment = waterHolder[head]) {
            null -> logger.error("重复释放事务？？？？？？？？？？？？？？？？？？？？？")
            else -> {
                val releaseIndex = trxSegment.release(TrxId)
                logger.debug("事务 $TrxId 已经释放")

                var releaseSegment = false
                var releaseLowWaterMark = false

                // 只有当 trxBitMap == releaseBitMap
                // 并且 trxBitMap 最后一位为 1
                if (trxSegment.trxBitMap == trxSegment.releaseBitMap && trxSegment.trxBitMap and IntervalMinusMask == IntervalMinusMask) {
                    waterHolder.remove(head)
                    releaseSegment = true
                }

                // 如果当前操作的是最小的段，最小段发生操作，则有可能会更新最小水位，此时需要推送一下当前提交的最小事务
                val isMinSeg = waterHolder.firstEntry()?.value?.let { it == trxSegment } ?: false
                if (isMinSeg) {
                    logger.debug("当前最小事务 $TrxId 已经释放")
                    logger.debug("${lowWaterMark()}")
                    releaseLowWaterMark = true
                }
                return WaterReleaseResult(releaseSegment, releaseLowWaterMark)
            }
        }

        return WaterReleaseResult()
    }

    /**
     * 获取的最小的有效的事务
     */
    fun lowWaterMark(): Long {
        val min = waterHolder.firstEntry()?.value?.minTrx() ?: TrxAllocator.StartTrx
        return min
    }

}