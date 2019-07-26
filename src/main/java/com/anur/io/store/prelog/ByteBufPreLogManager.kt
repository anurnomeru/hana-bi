package com.anur.io.store.prelog

import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.lock.ReentrantReadWriteLocker
import com.anur.exception.LogException
import com.anur.io.store.common.PreLogMeta
import com.anur.io.store.log.LogManager
import com.anur.io.store.operationset.ByteBufferOperationSet
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.Supplier

/**
 * Created by Anur IjuoKaruKas on 2019/7/12
 *
 * 负责写入预日志，并操作将预日志追加到日志中
 */
object ByteBufPreLogManager : ReentrantReadWriteLocker() {

    private val preLog: ConcurrentSkipListMap<Long, ByteBufPreLog> = ConcurrentSkipListMap()

    private val logger = LoggerFactory.getLogger(ByteBufPreLogManager::class.java)

    /**
     * 当前已经提交的 offset
     */
    private var commitOffset: GenerationAndOffset? = null

    /**
     * 预日志最后一个 offset
     */
    private var preLogOffset: GenerationAndOffset? = null

    init {
        this.commitOffset = LogManager.initial
        this.preLogOffset = commitOffset
        logger.info("预日志初始化成功，预日志将由 {} 开始", commitOffset!!.toString())
    }

    /**
     * 供 leader 写入使用
     */
    fun cover(GAO: GenerationAndOffset) {
        writeLocker {
            commitOffset = GAO
            preLogOffset = GAO
        }
    }

    /**
     * 获取当前副本同步到的最新的 preLog GAO
     */
    fun getPreLogGAO(): GenerationAndOffset {
        return readLockSupplierCompel(Supplier { preLogOffset!! })
    }

    /**
     * 获取当前副本同步到的最新的 commit GAO
     */
    fun getCommitGAO(): GenerationAndOffset {
        CommitProcessManager.discardInvalidMsg()
        return readLockSupplierCompel(Supplier { commitOffset!! })
    }

    /**
     * 此添加必须保证一次调用中，ByteBufferOperationSet 所有的操作日志都在同一个世代，实际上也确实如此
     */
    fun append(generation: Long, byteBufferOperationSet: ByteBufferOperationSet) {
        writeLocker {
            /* 简单检查 */
            if (generation < preLogOffset!!.generation) {
                logger.error("追加到预日志的日志 generation {} 小于当前预日志 generation {}，追加失败！", generation, preLogOffset!!.getGeneration())
                return@writeLocker
            }

            val byteBufPreLogOperated = preLog.compute(generation) { _, byteBufPreLog ->
                byteBufPreLog ?: ByteBufPreLog(generation)
            }

            val iterator = byteBufferOperationSet.iterator()
            var lastOffset = -1L

            while (iterator.hasNext()) {
                val oao = iterator.next()

                val oaoOffset = oao.offset
                val currentLast = preLogOffset!!.offset

                if (oaoOffset <= currentLast) {
                    logger.error("追加到预日志的日志 offset $oaoOffset 小于当前预日志 offset $currentLast，追加失败！！")
                    break
                } else {
                    logger.info("追加预日志 offset $oaoOffset 成功，当前预日志 offset 为 $currentLast")
                }

                byteBufPreLogOperated!!.append(oao.operation, oao.offset)

                lastOffset = oaoOffset
            }

            if (lastOffset != -1L) {
                val before = preLogOffset
                preLogOffset = GenerationAndOffset(generation, lastOffset)
                logger.debug("本地追加了预日志，由 {} 更新至 {}", before.toString(), preLogOffset.toString())
            }
        }
    }

    /**
     * 将此 offset 往后的数据都提交到本地
     */
    fun commit(GAO: GenerationAndOffset) {
        writeLocker {
            // 先与本地已经提交的记录做对比，只有大于本地副本提交进度时才进行commit
            val compareResult = GAO.compareTo(commitOffset)

            // 需要提交的进度小于等于preLogOffset
            if (compareResult <= 0) {
                logger.debug("收到来自 Leader 节点的无效 Commit 请求 => {}，本地预日志 commit 进度 {} 已经大于等于此请求。", GAO.toString(), commitOffset.toString())
                return@writeLocker
            } else {

                val canCommit = readLockSupplierCompel(Supplier { if (GAO > preLogOffset) preLogOffset else GAO })

                if (canCommit == commitOffset) {
                    logger.debug("收到来自 Leader 节点的有效 Commit 请求，本地预日志最大为 {} ，故可提交到 {} ，但本地已经提交此进度。", preLogOffset.toString(), canCommit!!.toString())
                } else {
                    logger.info("收到来自 Leader 节点的有效 Commit 请求，本地预日志最大为 {} ，故可提交到 {}", preLogOffset.toString(), canCommit!!.toString())

                    val preLogMeta = getBefore(canCommit) ?: throw LogException("有bug请注意排查！！，不应该出现这个情况")

                    val byteBufferOperationSet = ByteBufferOperationSet.cast((preLogMeta.oao))

                    // 追加到磁盘
                    for (bos in byteBufferOperationSet) {
                        LogManager.append(bos, GAO.generation, preLogMeta.startOffset, preLogMeta.endOffset)
                    }

                    // 强制刷盘
                    LogManager.activeLog().flush(preLogMeta.endOffset)

                    logger.info("本地预日志 commit 进度由 {} 更新至 {}", commitOffset.toString(), canCommit.toString())
                    commitOffset = canCommit
                    discardBefore(canCommit)
                }
            }
        }
    }

    /**
     * 获取当前这一条之前的预日志（包括这一条）
     */
    private fun getBefore(GAO: GenerationAndOffset): PreLogMeta? {
        return this.readLockSupplier(Supplier {
            val gen = GAO.generation
            val offset = GAO.offset
            val head = preLog.headMap(gen, true)

            if (head == null || head.size == 0) {
                throw LogException("获取预日志时：世代过小或者此世代还未有预日志")
            }

            val byteBufPreLog = head.firstEntry()
                .value

            byteBufPreLog.getBefore(offset)
        })
    }

    /**
     * 丢弃掉一些预日志消息（批量丢弃，包括这一条）
     */
    private fun discardBefore(GAO: GenerationAndOffset) {
        this.writeLockSupplier(Supplier {
            val gen = GAO.generation
            val offset = GAO.offset
            val head = preLog.headMap(gen, true)

            if (head == null || head.size == 0) {
                throw LogException("获取预日志时：世代过小或者此世代还未有预日志")
            }

            val byteBufPreLog = head.firstEntry()
                .value
            byteBufPreLog.discardBefore(offset)
        })
    }
}