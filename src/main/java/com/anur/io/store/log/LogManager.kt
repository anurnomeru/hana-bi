package com.anur.io.store.log

import com.anur.config.LogConfigHelper
import com.anur.core.elect.ElectMeta
import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.elect.operator.ElectOperator
import com.anur.core.listener.EventEnum
import com.anur.core.listener.HanabiListener
import com.anur.core.lock.ReentrantReadWriteLocker
import com.anur.core.struct.base.Operation
import com.anur.exception.LogException
import com.anur.io.store.common.FetchDataInfo
import com.anur.io.store.common.LogCommon
import com.anur.io.store.common.PreLogMeta
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier
import kotlin.math.max

/**
 * Created by Anur IjuoKaruKas on 2019/7/12
 */
object LogManager {

    private val logger = LoggerFactory.getLogger(LogManager::class.java)

    /** 显式锁 */
    private val appendLock = ReentrantReadWriteLocker()

    /** 显式锁 */
    private val explicitLock = ReentrantReadWriteLocker()

    /** 管理所有 Log  */
    private val generationDirs = ConcurrentSkipListMap<Long, Log>()

    /** 基础目录  */
    private val baseDir = File(LogConfigHelper.getBaseDir() + "\\log\\aof\\")

    /** 初始化时，最新的 Generation 和 Offset  */
    val initial: GenerationAndOffset = init()

    /** 最新的那个 GAO  */
    @Volatile
    private var currentGAO = initial

    /** Leader节点比较特殊，在集群不可用以后，要销毁掉集群内未提交的操作日志 */
    @Volatile
    private var isLeaderCurrent: Boolean = false

    /**
     * 加载既存的目录们
     */
    private fun init(): GenerationAndOffset {
        HanabiListener.register(EventEnum.CLUSTER_VALID) {
            if (ElectMeta.isLeader) {
                isLeaderCurrent = true
            }
        }

        baseDir.mkdirs()

        var latestGeneration = 0L

        for (file in baseDir.listFiles()!!) {
            if (!file.isFile) {
                latestGeneration = max(latestGeneration, Integer.valueOf(file.name).toLong())
            }
        }

        val init: GenerationAndOffset

        // 只要创建最新的那个 generation 即可
        try {
            val latest = Log(latestGeneration, createGenDirIfNEX(latestGeneration))
            generationDirs[latestGeneration] = latest

            init = GenerationAndOffset(latestGeneration, latest.currentOffset)
        } catch (e: IOException) {
            throw LogException("操作日志初始化失败，项目无法启动")
        }

        logger.info("初始化日志管理器，当前最大进度为 {}", init.toString())

        /**
         * 当集群不可用
         *
         *  - 关闭追加入口
         *  - 必须抛弃未 commit 部分，避免将未提交的数据恢复到集群日志中
         */
        HanabiListener.register(EventEnum.CLUSTER_INVALID) {
            appendLock.switchOff()
            logger.info("追加入口关闭~")

            if (isLeaderCurrent) {
                CommitProcessManager.discardInvalidMsg()
            }
        }

        /**
         * 当集群日志恢复完毕
         *
         *  - 开放追加入口
         */
        HanabiListener.register(EventEnum.RECOVERY_COMPLETE) {

            appendLock.switchOn()
            logger.info("追加入口启用~")
        }

        appendLock.switchOff()
        logger.info("追加入口关闭~")

        return init
    }

    /**
     * 追加操作日志到磁盘，如果集群不可用，追加将阻塞
     */
    fun appendUntilClusterValid(operation: Operation) {
        appendLock.writeLocker {
            explicitLock.writeLocker {
                val operationId = ElectOperator.getInstance()
                    .genOperationId()

                currentGAO = operationId

                val log = maybeRoll(operationId.generation)
                log.append(operation, operationId.offset)
            }
        }
    }

    /**
     * 追加操作日志到磁盘，如果集群不可用，不会阻塞，供内部集群恢复时调用
     */
    fun append(preLogMeta: PreLogMeta, generation: Long, startOffset: Long, endOffset: Long) {
        explicitLock.writeLocker {
            val log = maybeRoll(generation)

            log.append(preLogMeta, startOffset, endOffset)

            currentGAO = GenerationAndOffset(generation, endOffset)
        }
    }

    /**
     * 在 append 操作时，如果世代更新了，则创建新的 Log 管理
     */
    private fun maybeRoll(generation: Long): Log {
        val current = activeLog()
        if (generation > current.generation) {
            val dir = createGenDirIfNEX(generation)
            val log: Log
            try {
                log = Log(generation, dir)
            } catch (e: IOException) {
                throw LogException("创建世代为 $generation 的操作日志管理文件 Log 失败")
            }

            generationDirs[generation] = log
            return log
        } else if (generation < current.generation) {
            throw LogException("不应在添加日志时获取旧世代的 Log")
        }

        return current
    }

    /**
     * 创建世代目录
     */
    fun createGenDirIfNEX(generation: Long): File {
        return LogCommon.dirName(baseDir, generation)
    }

    /**
     * 获取最新的一个日志分片管理类 Log
     */
    fun activeLog(): Log {
        return generationDirs.lastEntry().value
    }

    /**
     * 只返回某个 segment 的往后所有消息，需要客户端轮询拉数据（包括拉取本身这条消息）
     *
     * 先获取符合此世代的首个 Log ，称为 needLoad
     *
     * ==>      循环 needLoad，直到拿到首个有数据的 LogSegment，称为 needToRead
     *
     * 如果拿不到 needToRead，则进行递归
     */
    fun getAfter(GAO: GenerationAndOffset): FetchDataInfo? {
        return explicitLock.readLockSupplier(Supplier {
            val gen = GAO.generation
            val offset = GAO.offset

            //  GAO 过大直接返回null
            if (GAO > currentGAO) {
                return@Supplier null
            }

            /*
             * 如果不存在此世代，则加载此世代
             */
            if (!generationDirs.containsKey(gen)) {
                loadGenLog(gen)

                /*
                 * 如果还是不存在，则拉取更大世代
                 */
                if (!generationDirs.containsKey(gen)) {
                    return@Supplier getAfter(GenerationAndOffset(gen + 1, 0))
                }
            }

            val tailMap = generationDirs.tailMap(gen, true)
            if (tailMap == null || tailMap.size == 0) {
                //此世代还未有预日志
                return@Supplier null
            }

            // 取其最小者
            val firstEntry = tailMap.firstEntry()

            val needLoadGen = firstEntry.key
            val needLoadLog = firstEntry.value

            val logSegmentIterable =
                needLoadLog.getLogSegments(offset, Long.MAX_VALUE).iterator()

            var needToRead: LogSegment? = null
            while (logSegmentIterable.hasNext()) {
                val tmp = logSegmentIterable.next()
                if (needLoadLog.currentOffset != tmp.baseOffset) {// 代表这个 LogSegment 一条数据都没 append
                    needToRead = tmp
                    break
                }
            }

            return@Supplier if (needToRead == null) {
                getAfter(GenerationAndOffset(needLoadGen + 1, offset))
            } else needToRead.read(needLoadGen, offset, Long.MAX_VALUE, Int.MAX_VALUE)
        })
    }

    /**
     * 如果某世代日志还未初始化，则将其初始化，并加载部分到内存
     */
    @Synchronized
    fun loadGenLog(gen: Long): Log? {
        if (!generationDirs.containsKey(gen) && LogCommon.dirName(baseDir, gen).exists()) {
            generationDirs[gen] = Log(gen, createGenDirIfNEX(gen))
        }
        return generationDirs[gen]
    }

    /**
     * 丢弃某个 GAO 往后的所有消息
     */
    fun discardAfter(GAO: GenerationAndOffset) {
        explicitLock.writeLocker {
            for (i in GAO.generation..currentGAO.generation) {
                loadGenLog(i)
            }

            var result = doDiscardAfter(GAO)
            while (result) {
                result = doDiscardAfter(GAO)
            }
            currentGAO = GAO
        }
    }


    /**
     * 真正丢弃执行者，注意运行中不要乱调用这个，因为没加锁
     */
    private fun doDiscardAfter(GAO: GenerationAndOffset): Boolean {
        val gen = GAO.generation
        val offset = GAO.offset

        val tailMap = generationDirs.tailMap(gen, true)
        if (tailMap == null || tailMap.size == 0) {
            // 世代过大或者此世代还未有预日志
            return false
        }

        // 取其最大者
        val lastEntry = tailMap.lastEntry()

        val needDeleteGen = lastEntry.key
        val needDeleteLog = lastEntry.value

        // 若存在比当前更大的世代的日志，将其全部删除
        val deleteAll = when {
            needDeleteGen == gen -> false
            needDeleteGen > gen -> true
            else -> throw LogException("注意一下这种情况，比较奇怪！")
        }

        return if (deleteAll) {
            val logSegmentIterable =
                needDeleteLog.getLogSegments(0, Long.MAX_VALUE).iterator()

            logger.info("当前需删除 $GAO 往后的日志，故世代 $needDeleteGen 日志将全部删去")
            logSegmentIterable.forEach {
                it.fileOperationSet.fileChannel.close()
                val needToDelete = it.fileOperationSet.file
                logger.debug("删除日志分片 ${needToDelete.absoluteFile} "
                    + (if (needToDelete.delete()) "成功" else "失败")
                    + "。删除对应索引文件"
                    + if (it.offsetIndex.delete()) "成功" else "失败")
            }

            val dir = needDeleteLog.dir
            val success: Boolean = needDeleteLog.dir.delete()
            logger.debug("删除目录 $dir" + if (success) {
                generationDirs.remove(needDeleteGen)
                "成功"
            } else "失败")
            true
        } else {
            logger.info("当前需删除 $GAO 往后的日志，故世代 $needDeleteGen 日志将部分删去")
            val logSegmentIterable =
                needDeleteLog.getLogSegments(offset, Long.MAX_VALUE).iterator()

            logSegmentIterable.forEach {
                if (it.baseOffset <= offset && offset <= it.lastOffset(needDeleteGen)) {
                    it.truncateTo(offset)
                    logger.debug("删除日志分片 ${it.fileOperationSet.file} 中大于等于 $offset 的记录移除")
                    needDeleteLog.currentOffset = offset
                } else {
                    it.fileOperationSet.fileChannel.close()
                    val needToDelete = it.fileOperationSet.file
                    logger.debug("删除日志分片 ${needToDelete.absoluteFile}" + if (needToDelete.delete()) "成功" else "失败")
                }
            }
            false
        }
    }
}

