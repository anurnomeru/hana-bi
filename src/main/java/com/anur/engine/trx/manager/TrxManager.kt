package com.anur.engine.trx.manager

import com.anur.core.lock.rentrant.ReentrantReadWriteLocker
import com.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.function.Supplier


/**
 * Created by Anur IjuoKaruKas on 2019/10/15
 *
 * 事务管理器
 */
object TrxManager {

    private const val interval = 64L
    private const val intervalMinusOne = 63

    private val logger: Logger = LoggerFactory.getLogger(TrxFreeQueuedSynchronizer::class.java)

    private val locker = ReentrantReadWriteLocker()

    private var nowTrx: Long = -1L
//            Long.MIN_VALUE

    private val waterHolder = TreeMap<Long, TrxSegment>(kotlin.Comparator { o1, o2 -> o1.compareTo(o2) })

    /**
     * 申请一个递增的事务id
     */
    fun allocate(): Long {
        return locker.writeLockSupplierCompel(Supplier {
            val trx = nowTrx + 1
            nowTrx = trx

            val index = trx / interval

            // 将事务扔进水位
            if (!waterHolder.contains(index)) waterHolder[index] = TrxSegment(trx)
            waterHolder[index]!!.acquire(trx)

            return@Supplier trx
        })
    }

    /**
     * 释放一个事务
     */
    fun releaseTrx(anyElse: Long) {
        locker.writeLocker() {
            val index = anyElse / interval

            when (val trxSegment = waterHolder.get(index)) {
                null -> logger.error("重复释放事务？？？？？？？？？？？？？？？？？？？？？")
                else -> {
                    trxSegment.release(anyElse)
                    if (trxSegment.trxBitMap == 0L && waterHolder.higherEntry(index) != null) {
                        waterHolder.remove(index)
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

        val start: Long = anyElse - anyElse % interval

        init {
            acquire(anyElse)
        }

        fun minTrx(): Long {
            return start + minIndex + 1
        }

        fun acquire(trxId: Long) {
            val index = ((interval - 1) and trxId).toInt()
            val mask = 1L.shl(index)
            trxBitMap = trxBitMap or mask
        }

        fun release(trxId: Long) {
            val index = ((interval - 1) and trxId).toInt()
            val mask = 1L.shl(index)
            trxBitMap = mask.inv() and trxBitMap

            // 计算当前最小的 index
            if (trxBitMap == 0L && index == intervalMinusOne) {
                minIndex = index
            } else {
                for (i in 0 until 64) {
                    val minIndexNeo = 1L.shl(i)

                    if (minIndexNeo and trxBitMap == minIndexNeo) {
                        minIndex = i - 1
                        break
                    }
                }
            }
        }
    }
}


fun main() {
//    for (i in 0 until 100036) {
//        TrxManager.allocate()
//    }
//    println(TrxManager.minTrx())
//
//    for (i in 0 until 100000) {
//        TrxManager.releaseTrx(i.toLong())
//    }
//    println(TrxManager.minTrx())

    for (i in 0 until 100036) {
        TrxManager.allocate()
    }
    println(TrxManager.minTrx())

    for (i in 0 until 100000) {
        TrxManager.releaseTrx(i.toLong())
    }
    println(TrxManager.minTrx())
}