package com.anur.engine.trx.lock

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.LinkedList

/**
 * Created by Anur IjuoKaruKas on 2019/9/26
 */
object TrxFreeQueuedSynchronizer {

    private val logger: Logger = LoggerFactory.getLogger(TrxFreeQueuedSynchronizer::class.java)

    /**
     * 标识 key 上的锁
     */
    private val lockKeeper: MutableMap<String, MutableList<Long>> = mutableMapOf()

    /**
     * 存储那些需要被唤醒的执行内容
     */
    private val trxHolderMap: MutableMap<Long, TrxHolder> = mutableMapOf()

    /**
     * 仅广义上的互斥锁需要加锁 (此方法必须串行)
     */
    fun acquire(trxId: Long, key: String, whatEverDo: () -> Unit) {
        // 创建或者拿到之前注册的事务，并注册持有此key的锁
        val trxHolder = trxHolderMap.compute(trxId) { _, th -> th ?: TrxHolder(trxId) }!!.also { it.holdKeys.add(key) }

        // 先去排队
        val waitQueue = lockKeeper.compute(key) { _, ll -> (ll ?: LinkedList()).also { if (!it.contains(trxId)) it.add(trxId) } }!!

        when (waitQueue.first()) {
            // 代表此键无锁
            trxId -> {
                whatEverDo.invoke()
                logger.trace("事务 $trxId 成功获取或重入位于键 $key 上的锁，并成功进行了操作")
            }

            // 代表有锁
            else -> {
                trxHolder.undoEvent.compute(key) { _, undoList -> (undoList ?: mutableListOf()).also { it.add(whatEverDo) } }
                logger.trace("事务 $trxId 无法获取位于键 $key 上的锁，将等待键上的前一个事务唤醒，且挂起需执行的操作。")
            }
        }
    }


    /**
     * 释放 trxId 下的所有键锁
     */
    fun release(trxId: Long, doWhileCommit: () -> Unit) {

        if (!trxHolderMap.containsKey(trxId)) {
            logger.error("事务未创建或者已经提交，trxId: $trxId")
            return
        }

        val trxHolder = trxHolderMap[trxId]!!
        if (trxHolder.undoEvent.isNotEmpty()) {
            logger.debug("事物 $trxId 还在阻塞等待唤醒，暂无法释放！故释放操作将挂起，等待唤醒。")
            trxHolder.doWhileCommit = doWhileCommit
        } else {
            doWhileCommit.invoke()
            val holdKeys = trxHolder.holdKeys

            // 移除所有锁
            holdKeys.forEach {
                val queue = lockKeeper[it]
                if (queue == null) logger.error("喵喵喵？？")
                else {
                    if (queue.first() != trxId) {
                        logger.error("喵喵喵？？x2")
                        return
                    } else {
                        queue.remove(trxId)
                    }
                }
            }

            logger.debug("事务 $trxId 已经成功释放锁")

            // 注销此事务
            trxHolderMap.remove(trxId)

            // 通知其他事务
            holdKeys.forEach { notify(it) }

            if (lockKeeper.isEmpty()) {
                logger.info("全部释放")
            }
        }
    }

    /**
     * 唤醒挂起的事务操作～～
     */
    private fun notify(key: String) {
        val mutableList = lockKeeper[key]!!
        if (mutableList.isEmpty()) {
            lockKeeper.remove(key)
            return
        } else {
            val first = mutableList.first()
            val trxHolder = trxHolderMap[first]!!

            logger.trace("挂起的事务操作 trxId: $first, key = $key 被唤醒，并执行。")
            trxHolder.undoEvent[key]!!.forEach { it.invoke() }
            trxHolder.undoEvent.remove(key)

            // 如果当前被唤醒的事务已经执行完所有挂起的事务了，则直接将其释放
            if (trxHolder.undoEvent.isEmpty()) trxHolder.doWhileCommit?.also {
                logger.debug("挂起的事务操作 trxId: $first 的所有待执行任务已经执行完毕，且已经注册了释放事件，将触发此释放事件。")
                release(first, it)
            }
        }
    }
}