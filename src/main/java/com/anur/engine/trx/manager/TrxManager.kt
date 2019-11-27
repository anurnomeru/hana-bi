package com.anur.engine.trx.manager

import com.anur.core.lock.rentrant.ReentrantReadWriteLocker
import com.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.LinkedBlockingDeque
import java.util.function.Supplier
import kotlin.math.absoluteValue
import kotlin.math.max
import kotlin.math.min

/**
 * Created by Anur IjuoKaruKas on 2019/10/15
 *
 * 事务管理器，包括获取自增的事务id，释放事务id，获取当前活跃的最小事务等
 */
object TrxManager {

    const val Interval = 64L
    private const val IntervalMinusOne = 63

    private val logger: Logger = LoggerFactory.getLogger(TrxFreeQueuedSynchronizer::class.java)

    private val waterHolder = TreeMap<Long, TrxSegment>(kotlin.Comparator { o1, o2 -> o1.compareTo(o2) })
    private val lockHolder = mutableMapOf<Long, ReentrantReadWriteLocker>()
    private val recycler = mutableListOf<ReentrantReadWriteLocker>()
    private val notifyQueue = LinkedBlockingDeque<Long>()

    /**
     * 申请并激活一个递增的事务id，代表这个事务id是已经正式开始投入使用了，处于 “待提交水位”
     */
    fun acquireTrx(): Long {
        val allocate = TrxAllocator.allocate()
        return acquireTrx(allocate)
    }

    /**
     * 激活一个递增的事务id，代表这个事务id是已经正式开始投入使用了，处于 “待提交水位”
     */
    fun acquireTrx(anyElse: Long): Long {
        val head = genSegmentHead(anyElse)
        return acquireLocker(head).writeLockSupplierCompel(Supplier {
            // 将事务扔进水位
            if (!waterHolder.contains(head)) waterHolder[head] = TrxSegment(anyElse)
            waterHolder[head]!!.acquire(anyElse)

            logger.trace("事务 $anyElse 已经激活")
            return@Supplier anyElse
        })
    }

    /**
     * 释放一个事务，并可能刷新最低水位
     */
    fun releaseTrx(anyElse: Long) {
        val head = genSegmentHead(anyElse)

        acquireLocker(head).writeLocker() {
            when (val trxSegment = waterHolder[head]) {
                null -> logger.error("重复释放事务？？？？？？？？？？？？？？？？？？？？？")
                else -> {
                    val releaseIndex = trxSegment.release(anyElse)
                    logger.debug("事务 $anyElse 已经释放")

                    // 当事务段都为0，且
                    // （有比当前更大的head，才可以销毁这个head（代表不会有更多的申请来到这里）
                    // 或者
                    // 释放的是最后一个index）
                    // 将相关用到的数据内存释放掉
                    if (trxSegment.trxBitMap == 0L && (waterHolder.higherEntry(head) != null || releaseIndex == IntervalMinusOne)) {
                        waterHolder.remove(head)
                        destroyLocker(head)
                    }

                    // 如果当前操作的是最小的段，最小段发生操作，则推送一下当前提交的最小事务
                    val isMinSeg = waterHolder.firstEntry()?.value?.let { it == trxSegment } ?: false
                    if (isMinSeg) {
                        logger.debug("当前最小事务 $anyElse 已经释放")
                        logger.debug("${minTrx()}")
                        notifyQueue.push(minTrx())
                    }
                }
            }
        }
    }

    /**
     * TrxManager 的所有操作都是要获取锁的，这里的锁使用分段锁，间隔为64个事务
     *
     * 这里传入事务“头”，也就是64取余，来获取一个读写锁
     */
    private fun acquireLocker(head: Long): ReentrantReadWriteLocker {
        return synchronized(this) {
            lockHolder.compute(head) { _, lock ->
                lock ?: let {
                    return@let if (recycler.size > 0) {
                        val first = recycler.first()
                        recycler.remove(first)
                        first
                    } else {
                        ReentrantReadWriteLocker()
                    }
                }
            }
        }!!
    }

    /**
     * 分段锁销毁，销毁后将锁丢回 recycler 方便重复使用，因为锁这个东西实际没必要一直创建新的
     */
    private fun destroyLocker(head: Long) {
        synchronized(this) {
            lockHolder.remove(head)?.let { recycler.add(it) }
        }
    }

    /**
     * 获取的最小的有效的事务
     */
    fun minTrx(): Long {
        return waterHolder.firstEntry()?.value?.minTrx() ?: TrxAllocator.StartTrx
    }

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

    /**
     * 一旦有更大的事务被提交，它就会开始工作
     */
    fun takeNotify(): Long {
        return notifyQueue.take()
    }

    /**
     * 为了避免事务太多，列表太大，故采用分段
     */
    class TrxSegment(anyElse: Long) {

        /**
         * 标记此事务第一位的事务id，比如为 100
         * 那么 trxBitMap 0011 代表 100 101 事务已经激活
         */
        val start: Long = genSegmentHead(anyElse) * Interval

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

        init {
            acquire(anyElse)
        }

        fun minTrx(): Long? {
            return if (releaseBitMap == 0L) {
                null
            } else {
                var result = 0
                var mask = 1L
                while (result < Interval) {
                    if (mask and releaseBitMap != mask) {
                        break
                    }
                    result++
                    mask = mask shl 1
                }
                start + max(result - 1, 0)
            }
        }

        fun acquire(trxId: Long) {
            val index = calcIndex(trxId)
            val mask = 1L.shl(index)
            trxBitMap = trxBitMap or mask

//            if (index < minIndexAc || minIndexAc == -1) {
//                minIndexAc = index
//            }
        }

        fun release(trxId: Long): Int {
            val index = calcIndex(trxId)
            val mask = 1L.shl(index)
            releaseBitMap = trxBitMap or mask

//            trxBitMap = mask.inv() and trxBitMap
//            // 计算当前最小的 index
//            if (trxBitMap == 0L && index == IntervalMinusOne) {
//                minIndex = index
//            } else if (minIndex != -1 && trxBitMap == 0L) {
//                if (minIndex != minIndexAc) {
//                    logger.error("触发了！！ $minIndex - $minIndexAc")
//                }
//                minIndex = minIndexAc
//            } else {
//                for (i in 0 until 64) {
//                    val minIndexNeo = 1L.shl(i)
//
//                    if (minIndexNeo and trxBitMap == minIndexNeo) {
//                        minIndex = i - 1
//                        break
//                    }
//                }
//            }
            return index
        }

        private fun calcIndex(trxId: Long): Int {
            return ((Interval - 1) and trxId).toInt()
        }
    }
}
