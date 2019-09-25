package com.anur.core.lock.free

import com.anur.util.HanabiExecutors
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.LinkedList
import java.util.function.Consumer

/**
 * Created by Anur IjuoKaruKas on 2019/9/24
 *
 * 使用无锁来实现的一种互斥锁，严格意义上也不是锁 = =
 */
object TrxFreeQueuedLocker {

    private val logger: Logger = LoggerFactory.getLogger(TrxFreeQueuedLocker::class.java)

    /**
     * 标识 key 上的锁
     */
    private val lockKeeper: MutableMap<String, LinkedList<TrxHolder>> = mutableMapOf()

    /**
     * 存储那些需要被唤醒的执行内容
     */
    private val trxHolderMap: MutableMap<Long, TrxHolder> = mutableMapOf()

    /**
     * 仅广义上的互斥锁需要加锁 (此方法必须串行)
     */
    fun acquire(trxId: Long, key: String, whatEverDo: () -> Unit) {
        val trxHolder = trxHolderMap.compute(trxId) { _, l ->
            (l ?: TrxHolder(trxId)).also { th ->
                th.holdKeys.add(key)
            }
        }!!

        lockKeeper.compute(key) { _, queue ->
            (queue ?: LinkedList()).also { queueRefer ->
                val hasLock = queueRefer.takeIf { it.size > 0 }?.first?.let { it == trxHolder } ?: false
                if (!hasLock) {
                    queueRefer.add(trxHolder)
                }

                if (hasLock || queueRefer.size == 1) {
                    // 如果当前键还未加锁，则直接执行内容
                    HanabiExecutors.execute(Runnable { whatEverDo.invoke() })
                    if (hasLock) {
                        logger.debug("事务 $trxId 成功重入键 $key 的锁。")
                    } else {
                        logger.debug("事务 $trxId 成功获取到键 $key 的锁。")
                    }
                } else {
                    // 表示当前有一个内容还未执行
                    trxHolder.unProcessOperate++

                    // 如果当前键已经有锁，则将内容存下，相当于阻塞，等待唤醒
                    trxHolder.undoEvent.compute(key) { _, undoList ->
                        (undoList ?: mutableListOf()).also { it.add(whatEverDo) }
                    }
                    logger.debug("事务 $trxId 暂无法获取 $key 的锁，将进入阻塞队列挂起，并等待唤醒，且当前事务有 ${trxHolder.unProcessOperate} 个待唤醒任务队列。")
                }
            }
        }
    }

    /**
     * 提交，如果事务还未提交，则挂起(此方法必须串行)
     */
    fun commit(trxId: Long, doWhileCommit: () -> Unit) {
        val trxHolder = trxHolderMap[trxId]
        if (trxHolder == null) {
            logger.error("喵喵喵？一定是哪里有问题的说。")
        } else {
            if (trxHolder.unProcessOperate == 0) {// 代表没有任何被“阻塞”的操作
                var notifyTasks: MutableList<Runnable>? = null

                // 首先执行自己的提交任务
                HanabiExecutors.execute(Runnable { doWhileCommit.invoke() })

                // 首先移除自己
                trxHolderMap.remove(trxId)
                logger.debug("事务 $trxId 将释放所有锁，并逐个唤醒等待任务。")

                // 其次移除所有自己所在的 lockKeeper
                for (holdKey in trxHolder.holdKeys) {
                    lockKeeper[holdKey]?.also { queue ->
                        if (queue.first.trxId != trxId) {// 讲道理不应该出现！！
                            logger.debug("事务 $trxId 移除于键 $holdKey 上的锁失败，该锁队列还有 ${queue.size} 个等待的任务队列")
                            logger.error("喵喵喵？一定是哪里有问题的说 x 2。")
                        } else {
                            // 移除锁
                            queue.removeFirst()
                            logger.debug("事务 $trxId 释放位于键 $holdKey 上的锁")

                            if (queue.size == 0) {
                                lockKeeper.remove(holdKey)
                            } else {
                                logger.debug("事务 $trxId 移除于键 $holdKey 上的锁，该锁队列还有 ${queue.size} 个等待的任务队列")

                                // 迭代触发队列中第一个等待的操作
                                val nextOne = queue.first

                                nextOne.also { nextOneHolder ->
                                    nextOneHolder.undoEvent[holdKey]

                                        // 如果等待的包含此键，则触发其待执行任务
                                        ?.also {
                                            notifyTasks = (notifyTasks ?: mutableListOf()).also { l ->
                                                l.add(Runnable {
                                                    // 将其执行，并减少计数
                                                    it.forEach { doSomeThing -> doSomeThing.invoke() }
                                                    nextOneHolder.unProcessOperate -= it.size

                                                    logger.debug("事务 $trxId 唤醒事务 ${nextOne.trxId} 关于 $holdKey 的阻塞任务，并执行 ${it.size} 个挂在此键下的阻塞任务，且被唤醒事务还有 ${nextOneHolder.unProcessOperate} 个阻塞任务队列。")
                                                    nextOneHolder.undoEvent.remove(holdKey)

                                                    val nextOneCommitOperate = nextOneHolder.doWhileCommit
                                                    // 提交如果阻塞，则递归触发提交
                                                    if (nextOneHolder.unProcessOperate == 0 && nextOneCommitOperate != null) commit(nextOneHolder.trxId, nextOneCommitOperate)
                                                })
                                            }
                                        }
                                }
                            }
                        }
                    }
                }

                notifyTasks?.forEach { HanabiExecutors.execute(it) }
            } else {// 如果当前还有被“阻塞”的操作，则将提交后要做的事情记录下来
                trxHolder.doWhileCommit = doWhileCommit
                logger.debug("事务 $trxId 还未执行完毕，执行完毕后，将释放此事务。")
            }
        }

        if (lockKeeper.isEmpty()) {
            logger.info("所有锁释放完毕")
        }
    }
}
