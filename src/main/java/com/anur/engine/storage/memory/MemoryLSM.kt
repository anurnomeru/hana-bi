package com.anur.engine.storage.memory

import com.anur.core.log.Debugger
import com.anur.engine.storage.entry.ByteBufferHanabiEntry
import com.anur.engine.storage.entry.FileHanabiEntry

/**
 * Created by Anur IjuoKaruKas on 2019/10/23
 */
object MemoryLSM {

    val logger = Debugger(MemoryLSM.javaClass)

    /**
     * 一个块 block 为 4Kb，假定平均一个元素为 64 - 128 byte，所以平均一下能存 1024 个 key
     */
    private const val FullMemoryAccess = (1024 * 4) * 1024 * 16

    /**
     * 责任链第一个lsm容器
     */
    private var firstChain = MemoryLSMChain()

    private var chainCount = 1

    /**
     * 通过责任链去获取到数据
     */
    fun get(key: String): ByteBufferHanabiEntry? = firstChain.get(key)

    /**
     * compute，并更新空间
     */
    fun put(key: String, entry: ByteBufferHanabiEntry) {
        val expectedSizeOverHead = FileHanabiEntry.getExpectedSizeOverHead(key)
        val entryExpectedSize = entry.expectedSize
        val expectedSize = expectedSizeOverHead + entryExpectedSize

        when {
            /*
             * 因 hanabiEntry 过大， 单 k-v 映射  ->  一个块
             * 故将此数据单独存到一个 MemoryLSM中，并位列当前lsm树之后
             */
            expectedSize > FullMemoryAccess -> {
                val memoryLSMChain = MemoryLSMChain()
                memoryLSMChain.memoryAssess = expectedSize
                memoryLSMChain.dataKeeper[key] = entry


                memoryLSMChain.nextChain = firstChain.nextChain
                firstChain.nextChain = memoryLSMChain

                if (firstChain.dataKeeper.containsKey(key)) {
                    val remove = firstChain.dataKeeper.remove(key)!!
                    firstChain.memoryAssess -= (expectedSizeOverHead + remove.expectedSize)
                }
                chainCount++

                logger.info("由于 HanabiEntry 过大，MemoryLSM 将为其单独分配一个 block，已进行扩容，现拥有 {} 个 block", chainCount)
            }
            /*
             * 如果达到阈值，则创建新的lsm块
             */
            expectedSize + firstChain.memoryAssess > FullMemoryAccess -> {
                val memoryLSMChain = MemoryLSMChain()
                memoryLSMChain.memoryAssess = expectedSize
                memoryLSMChain.dataKeeper[key] = entry

                memoryLSMChain.nextChain = firstChain
                firstChain = memoryLSMChain

                chainCount++
                logger.info("在插入新 HanabiEntry size[{}] 后，block 大小 [{}] 将超过阈值 {}，" +
                        " MemoryLSM 将新增一个 block，已进行扩容，现拥有 {} 个 block", expectedSize, firstChain.nextChain!!.memoryAssess, FullMemoryAccess, chainCount)
            }
            /*
             * 普通情况
             */
            else -> {
                firstChain.dataKeeper.compute(key) { _, v ->
                    v?.also {
                        firstChain.memoryAssess -= (expectedSizeOverHead + v.expectedSize)
                    }
                    firstChain.memoryAssess += expectedSize
                    entry
                }
            }
        }
    }
}