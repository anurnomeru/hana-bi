package com.anur.io.store.log

import com.anur.config.LogConfigHelper
import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.elect.operator.ElectOperator
import com.anur.core.struct.base.Operation
import com.anur.exception.LogException
import com.anur.io.store.common.FetchDataInfo
import com.anur.io.store.common.LogCommon
import com.anur.io.store.operationset.ByteBufferOperationSet
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.util.concurrent.ConcurrentSkipListMap
import kotlin.math.max

/**
 * Created by Anur IjuoKaruKas on 2019/7/12
 */
object LogManager {

    private val logger = LoggerFactory.getLogger(LogManager::class.java)

    /** 管理所有 Log  */
    private val generationDirs = ConcurrentSkipListMap<Long, Log>()

    /** 基础目录  */
    private val baseDir = File(LogConfigHelper.getBaseDir() + "\\log\\aof\\")

    /** 初始化时，最新的 Generation 和 Offset  */
    val initial: GenerationAndOffset = init()

    /** 最新的那个 GAO  */
    @Volatile
    private var currentGAO = initial

    /**
     * 加载既存的目录们
     */
    private fun init(): GenerationAndOffset {
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
            val latest = Log(latestGeneration, createGenDirIfNEX(latestGeneration), 0)
            generationDirs[1L] = latest
            init = GenerationAndOffset(latestGeneration, latest.currentOffset)
        } catch (e: IOException) {
            throw LogException("操作日志初始化失败，项目无法启动")
        }

        logger.info("初始化日志管理器，当前最大进度为 {}", init.toString())
        return init
    }

    /**
     * 添加一条操作日志到磁盘的入口
     */
    fun append(operation: Operation) {
        val operationId = ElectOperator.getInstance()
            .genOperationId()

        currentGAO = operationId

        val log = maybeRoll(operationId.generation)
        log.append(operation, operationId.offset)
    }

    /**
     * 添加多条操作日志到磁盘的入口
     */
    fun append(byteBufferOperationSet: ByteBufferOperationSet, generation: Long, startOffset: Long, endOffset: Long) {
        val log = maybeRoll(generation)
        log.append(byteBufferOperationSet, startOffset, endOffset)

        currentGAO = GenerationAndOffset(generation, endOffset)
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
                log = Log(generation, dir, 0)
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
    private fun createGenDirIfNEX(generation: Long): File {
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
        val gen = GAO.generation
        val offset = GAO.offset

        val tail = generationDirs.tailMap(gen, true)
        if (tail == null || tail.size == 0) {
            // 世代过大或者此世代还未有预日志
            return null
        }

        val firstEntry = tail.firstEntry()

        val needLoadGen = firstEntry.key
        val needLoad = firstEntry.value

        val logSegmentIterable = needLoad.getLogSegments(offset, java.lang.Long.MAX_VALUE)
            .iterator()

        var needToRead: LogSegment? = null
        while (logSegmentIterable.hasNext()) {
            val tmp = logSegmentIterable.next()
            if (needLoad.currentOffset != tmp.baseOffset) {// 代表这个 LogSegment 一条数据都没 append
                needToRead = tmp
                break
            }
        }

        return if (needToRead == null) {
            getAfter(GenerationAndOffset(needLoadGen + 1, offset))
        } else needToRead.read(needLoadGen, offset, java.lang.Long.MAX_VALUE, Integer.MAX_VALUE)

    }
}