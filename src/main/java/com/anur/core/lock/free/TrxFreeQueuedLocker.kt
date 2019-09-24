package com.anur.core.lock.free

import com.anur.util.HanabiExecutors
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.LinkedList

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
            (queue ?: LinkedList()).also {
                it.add(trxHolder)

                if ((it.size > 0)) {
                    // 如果当前键还未加锁，则直接执行内容
                    HanabiExecutors.execute(Runnable { whatEverDo.invoke() })
                } else {
                    // 表示当前有一个内容还未执行
                    trxHolder.unProcessOperate++

                    // 如果当前键已经有锁，则将内容存下，相当于阻塞，等待唤醒
                    trxHolder.doWhileCommit = whatEverDo
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
                HanabiExecutors.execute(Runnable { doWhileCommit.invoke() })

                // 首先移除自己
                trxHolderMap.remove(trxId)

                // 其次移除所有自己所在的 lockKeeper
                for (holdKey in trxHolder.holdKeys) {
                    lockKeeper[holdKey]?.also { queue ->
                        if (queue.first.trxId != trxId) {// 讲道理不应该出现！！
                            logger.error("喵喵喵？一定是哪里有问题的说 x 2。")
                        } else {
                            // 移除锁
                            queue.removeFirst()
                            if (queue.size == 0) {
                                lockKeeper.remove(holdKey)
                            } else {

                                // 迭代触发队列中第一个等待的操作
                                val nextOne = queue.first
                                nextOne.also { trxHolder ->
                                    trxHolder.undoEvent[holdKey]?.also {
                                        // 将其执行，并减少计数
                                        HanabiExecutors.execute(Runnable { it.invoke() })
                                        trxHolder.unProcessOperate--
                                    }
                                }

                                val nextOneCommitOperate = nextOne.doWhileCommit
                                // 提交如果阻塞，则递归触发提交
                                if (nextOne.unProcessOperate == 0 && nextOneCommitOperate != null) commit(nextOne.trxId, nextOneCommitOperate)
                            }
                        }
                    }
                }

            } else {// 如果当前还有被“阻塞”的操作，则将提交后要做的事情记录下来
                trxHolder.doWhileCommit = doWhileCommit
            }
        }
    }
}
