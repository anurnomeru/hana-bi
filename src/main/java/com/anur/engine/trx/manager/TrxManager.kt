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
     * 申请一个事务id
     */
    fun allocateTrx(): Long {
        return TrxAllocator.allocate()
    }

    /**
     * 激活一个递增的事务id，代表这个事务id是已经正式开始投入使用了，处于 “待提交水位”
     * 长事务需要创建事务快照，以保证隔离性，短事务没必要创建快照
     */
    fun activateTrx(trxId: Long, createWaterMarkSnapshot: Boolean) {
        if (createWaterMarkSnapshot) {
            // 为每个事务生成一个事务快照，并注册
            val watermarkSnapshot = WaterMarker(waterHolder.lowWaterMark(), trxId, waterHolder.snapshot())
            WaterMarkRegistry.register(trxId, watermarkSnapshot)
        }

        val head = WaterHolder.genSegmentHead(trxId)
        acquireLocker(head).writeLockSupplier(Supplier {
            // 将事务扔进水位
            if (!waterHolder.activateTrx(trxId)) {
                logger.error("事务 [$trxId] 不应该被激活，因为已经激活过了")
            }
            logger.trace("事务 [$trxId] 已经激活")
        })
    }

    /**
     * 释放一个事务，并可能刷新最低水位
     */
    fun releaseTrx(trxId: Long) {
        val head = WaterHolder.genSegmentHead(trxId)
        acquireLocker(head).writeLocker() {
            // 释放锁
            val waterReleaseResult = waterHolder.releaseTrx(trxId)

            // 如果当前事务段已经用完，则将其锁回收
            if (waterReleaseResult.releaseSegment) destroyLocker(head)

            // 刷新低水位，以便将数据从 commitPart 推入 lsm 树
            if (waterReleaseResult.releaseLowWaterMark) notifyQueue.push(waterHolder.lowWaterMark())

            // 释放数位快照
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
