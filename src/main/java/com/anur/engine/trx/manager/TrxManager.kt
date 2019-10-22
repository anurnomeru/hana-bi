package com.anur.engine.trx.manager

import com.anur.core.lock.rentrant.ReentrantReadWriteLocker
import com.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import com.anur.engine.trx.manager.TrxManager.StartTrx
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.StringBuilder
import java.util.*
import java.util.function.Supplier
import kotlin.math.absoluteValue

/**
 * Created by Anur IjuoKaruKas on 2019/10/15
 *
 * 事务管理器
 */
object TrxManager {

    const val Interval = 64L
    const val IntervalMinusOne = 63
    const val StartTrx: Long = -190

    private val logger: Logger = LoggerFactory.getLogger(TrxFreeQueuedSynchronizer::class.java)

    private val locker = ReentrantReadWriteLocker()

    private var nowTrx: Long = StartTrx

    private val waterHolder = TreeMap<Long, TrxSegment>(kotlin.Comparator { o1, o2 -> o1.compareTo(o2) })

    fun genSegmentHead(trxId: Long): Long {
        return if (trxId < 0) {
            (trxId.absoluteValue - 1) / Interval
        } else {
            trxId / Interval
        }
    }

    /**
     * 申请一个递增的事务id
     */
    fun allocate(): Long {
        return locker.writeLockSupplierCompel(Supplier {
            val trx = nowTrx
            nowTrx++
            val head = genSegmentHead(trx)

            // 将事务扔进水位
            if (!waterHolder.contains(head)) waterHolder[head] = TrxSegment(trx)
            waterHolder[head]!!.acquire(trx)
            return@Supplier trx
        })
    }

    /**
     * 释放一个事务
     */
    fun releaseTrx(anyElse: Long) {
        locker.writeLocker() {
            val head = genSegmentHead(anyElse)
            when (val trxSegment = waterHolder[head]) {
                null -> logger.error("重复释放事务？？？？？？？？？？？？？？？？？？？？？")
                else -> {
                    trxSegment.release(anyElse)
                    if (trxSegment.trxBitMap == 0L && waterHolder.higherEntry(head) != null) {
                        waterHolder.remove(head)
                    }
                }
            }
        }
    }

    /**
     * 获取的最小的有效的事务
     */
    fun minTrx(): Long {
        return locker.readLockSupplierCompel(Supplier {
            val trxSegment = waterHolder.firstEntry()?.value
            return@Supplier trxSegment?.minTrx() ?: nowTrx
        })
    }


    /**
     * 为了避免事务太多，列表太大，故采用分段
     */
    class TrxSegment(anyElse: Long) {

        @Volatile
        var trxBitMap: Long = 0

        var minIndex = -1

        var minIndexAc = -1

        val start: Long = genSegmentHead(anyElse) * Interval

        init {
            acquire(anyElse)
        }

        fun minTrx(): Long {
            return start + minIndex + 1
        }

        fun acquire(trxId: Long) {
            val index = calcIndex(trxId)
            val mask = 1L.shl(index)
            trxBitMap = trxBitMap or mask

            if (index < minIndexAc || minIndexAc == -1) {
                minIndexAc = index
            }

            toBinaryStr(trxBitMap)
        }

        fun release(trxId: Long) {
            if (trxId == -301L) {
                println()
            }

            val index = calcIndex(trxId)
            val mask = 1L.shl(index)
            trxBitMap = mask.inv() and trxBitMap

            // 计算当前最小的 index
            if (trxBitMap == 0L && index == IntervalMinusOne) {
                minIndex = index
            } else if (minIndex != -1 && trxBitMap == 0L) {
                if (minIndex != minIndexAc) {
                    logger.error("触发了！！ ${minIndex} - ${minIndexAc}")
                }
                minIndex = minIndexAc
            } else {
                for (i in 0 until 64) {
                    val minIndexNeo = 1L.shl(i)

                    if (minIndexNeo and trxBitMap == minIndexNeo) {
                        minIndex = i - 1
                        break
                    }
                }
            }

            toBinaryStr(trxBitMap)
        }

        private fun calcIndex(trxId: Long): Int {
            return ((Interval - 1) and trxId).toInt()
        }
    }
}

fun toBinaryStr(long: Long) {
    println(toBinaryStrIter(long, 63, StringBuilder()).toString())
}

fun toBinaryStrIter(long: Long, index: Int, appender: StringBuilder): StringBuilder {
    if (index == -1) {
        return appender
    } else {
        var mask = 1L shl index
        if (mask and long == mask) {
            appender.append("1")
        } else {
            appender.append("0")
        }
        return toBinaryStrIter(long, index - 1, appender)
    }
}


fun main() {
    for (i in 0 until 123456) {
        TrxManager.allocate()
    }
    println(TrxManager.minTrx())

    for (i in 0 until 200) {
        TrxManager.releaseTrx(TrxManager.StartTrx + i.toLong())
    }
    println(TrxManager.minTrx())
    println(TrxManager.minTrx() - (TrxManager.StartTrx))


}
