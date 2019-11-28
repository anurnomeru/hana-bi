package com.anur.engine.trx.manager

import com.anur.core.lock.rentrant.ReentrantReadWriteLocker
import com.anur.core.log.Debugger
import com.anur.engine.trx.watermark.WaterHolder
import com.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import com.anur.engine.trx.watermark.WaterMarkRegistry
import com.anur.engine.trx.watermark.WaterMarker
import java.util.concurrent.LinkedBlockingDeque
import java.util.function.Supplier

/**
 * Created by Anur IjuoKaruKas on 2019/10/15
 *
 * 事务管理器，包括获取自增的事务id，释放事务id，获取当前活跃的最小事务等
 */
object TrxManager {

    private val logger = Debugger(TrxFreeQueuedSynchronizer.javaClass)
    private val lockHolder = mutableMapOf<Long, ReentrantReadWriteLocker>()
    private val recycler = mutableListOf<ReentrantReadWriteLocker>()
    private val notifyQueue = LinkedBlockingDeque<Long>()
    private val waterHolder = WaterHolder()

    /**
     * 申请并激活一个递增的事务id，代表这个事务id是已经正式开始投入使用了，处于 “待提交水位”
     *
     * 长事务需要创建事务快照，以保证隔离性！
     */
    fun allocateTrx(createSnapshot: Boolean): Long {
        val allocate = TrxAllocator.allocate()

        // 为每个事务生成一个事务快照，并注册
        val watermarkSnapshot = if (createSnapshot) {
            WaterMarker(waterHolder.lowWaterMark(), allocate, waterHolder.snapshot())
        } else {
            WaterMarker.NONE
        }
        WaterMarkRegistry.register(allocate, watermarkSnapshot)

        val head = WaterHolder.genSegmentHead(allocate)
        return acquireLocker(head).writeLockSupplierCompel(Supplier {
            // 将事务扔进水位
            if (!waterHolder.activateTrx(allocate)) {
                logger.error("事务 [$allocate] 不应该被激活，因为已经激活过了")
            }

            logger.trace("事务 [$allocate] 已经激活")
            return@Supplier allocate
        })
    }

    /**
     * 释放一个事务，并可能刷新最低水位
     */
    fun releaseTrx(trxId: Long) {
        val head = WaterHolder.genSegmentHead(trxId)
        acquireLocker(head).writeLocker() {
            val waterReleaseResult = waterHolder.releaseTrx(trxId)
            if (waterReleaseResult.releaseSegment) destroyLocker(head)
            if (waterReleaseResult.releaseLowWaterMark) notifyQueue.push(waterHolder.lowWaterMark())
            WaterMarkRegistry.release(trxId)
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
     * 这个专门用于通知最低水位的刷新
     */
    fun takeNotify(): Long {
        return notifyQueue.take()
    }

    /**
     * 获取当前最低水位
     */
    fun lowWaterMark(): Long = waterHolder.lowWaterMark()


}
