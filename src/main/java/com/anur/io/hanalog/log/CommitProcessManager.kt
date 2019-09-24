package com.anur.io.hanalog.log

import com.anur.config.LogConfiguration
import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.lock.rentrant.ReentrantReadWriteLocker
import com.anur.io.hanalog.prelog.ByteBufPreLogManager
import org.slf4j.LoggerFactory
import java.io.File
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

/**
 * Created by Anur IjuoKaruKas on 2019/7/15
 *
 * 提交进度管理（担任过 leader 的节点需要用到）
 */
object CommitProcessManager : ReentrantReadWriteLocker() {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val dir = File(LogConfiguration.getBaseDir()!!)

    private val offsetFile = File(LogConfiguration.getBaseDir(), "commitOffset.temp")

    private val mmap: MappedByteBuffer

    private var commitGAO: GenerationAndOffset? = null

    init {
        dir.mkdirs()
        offsetFile.createNewFile()
        val raf = RandomAccessFile(offsetFile, "rw")
        raf.setLength((8 + 8).toLong())

        mmap = raf.channel.map(FileChannel.MapMode.READ_WRITE, 0, (8 + 8).toLong())

        load()
        discardInvalidMsg()
    }

    /**
     * 对于 leader 来说，由于不会写入 PreLog，故会持有未 commit 的消息
     *
     * 需要讲这些消息摈弃
     */
    fun discardInvalidMsg() {
        if (commitGAO != null && commitGAO != GenerationAndOffset.INVALID) {
            logger.info("检测到本节点曾是 leader 节点，需摒弃部分未 Commit 的消息")
            LogManager.discardAfter(commitGAO!!)
            ByteBufPreLogManager.cover(commitGAO!!)
            cover(GenerationAndOffset.INVALID)
            commitGAO?.let { logger.info("摒弃完毕，当前 节点 GAO -> $it") }
        }
    }


    /**
     * 加载 commitOffset.temp，获取集群提交进度（仅 leader 有效）
     */
    fun load(): GenerationAndOffset {
        writeLocker {
            if (commitGAO == null) {
                val gen = mmap.long
                val offset = mmap.long
                commitGAO = GenerationAndOffset(gen, offset)
                mmap.rewind()
            }
        }
        return commitGAO!!
    }

    /**
     * 覆盖提交进度
     */
    fun cover(GAO: GenerationAndOffset) {
        writeLocker {
            mmap.putLong(GAO.generation)
            mmap.putLong(GAO.offset)
            mmap.rewind()
            mmap.force()
            commitGAO = null
        }
    }
}