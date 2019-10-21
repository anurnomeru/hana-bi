package com.anur.engine.trx.manager

import com.anur.core.lock.rentrant.ReentrantReadWriteLocker
import com.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.Supplier


/**
 * Created by Anur IjuoKaruKas on 2019/10/15
 *
 * 事务管理器
 */
object TrxManager {

    private const val interval = 128L

    private val logger: Logger = LoggerFactory.getLogger(TrxFreeQueuedSynchronizer::class.java)

    private val locker = ReentrantReadWriteLocker()

    private var nowTrx: Long = Long.MIN_VALUE

    private val waterHolder = ConcurrentSkipListMap<Long, TrxSegment>()

    /**
     * 申请一个递增的事务id
     */
    fun allocate(): Long {
        val trx = locker.writeLockSupplierCompel(Supplier {
            val trx = nowTrx + 1
            nowTrx = trx

            val index = trx / interval

            // 将事务扔进水位
            if (!waterHolder.contains(index)) waterHolder[index] = TrxSegment(trx)
            waterHolder[index]!!.acquire(trx)

            return@Supplier trx
        })

        return trx
    }

    /**
     * 释放一个事务
     */
    fun releaseTrx(anyElse: Long) {
        locker.writeLocker() {
            val index = anyElse / interval

            when (val trxSegment = waterHolder[index]) {
                null -> logger.error("重复释放事务？？？？？？？？？？？？？？？？？？？？？")
                else -> {
                    trxSegment.release(anyElse)
                }
            }
        }
    }

    fun removeWaterHolderIfNeed() {

    }

    /**
     * 为了避免事务太多，列表太大，故采用分段
     */
    class TrxSegment(anyElse: Long) {

        private var trxBitMap = 0

        private var minTrxId = anyElse

        val start: Long = anyElse / interval

        fun acquire(trxId: Long) {
            val index = ((interval - 1) and trxId).toInt()
            val mask = 1.shl(index)
            trxBitMap = trxBitMap or mask

            if (trxId < minTrxId) {
                minTrxId = trxId
            }
        }

        fun release(trxId: Long) {
            val index = ((interval - 1) and trxId).toInt()
            val mask = 1.shl(index)
            trxBitMap = mask.inv() and trxBitMap
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as TrxSegment

            if (start != other.start) return false

            return true
        }

        override fun hashCode(): Int {
            return start.hashCode()
        }
    }
}