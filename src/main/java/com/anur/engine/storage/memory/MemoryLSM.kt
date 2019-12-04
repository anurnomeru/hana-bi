package com.anur.engine.storage.memory

import com.anur.engine.storage.core.HanabiEntry
import com.anur.engine.storage.disk.FileHanabiEntry

/**
 * Created by Anur IjuoKaruKas on 2019/10/23
 *
 * 一个块 block 为 4Kb，假定平均一个元素为 64 - 128 byte，所以平均一下能存 1024 个 key
 */
object MemoryLSM {

    /**
     * 硬盘块大小
     */
    private const val blockSize = 4096

    /**
     * 存储空间评估
     */
    private var memoryAssess: Long = 0

    private val dataKeeper = HashMap<String, FileHanabiEntry>()

    fun get(key: String): HanabiEntry? {
        return dataKeeper[key]?.entry
    }

    /**
     * compute，并更新空间
     */
    fun put(key: String, entry: HanabiEntry) {
        dataKeeper.compute(key) { _, v ->
            v?.also {
                memoryAssess -= it.getExpectedSize()
                memoryAssess += it.update(entry)
            } ?: FileHanabiEntry(key, entry).also {
                memoryAssess += it.getExpectedSize()
            }
        }
    }

    /**
     * 判断下是否需要一个新的lsm块
     */
    fun rollIfNeedNextLSMBlock(assess: Int) {
        if (assess + memoryAssess > blockSize) {
            if (assess > blockSize) {

            }
        }
    }
}