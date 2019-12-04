package com.anur.engine.storage.disk

import com.anur.engine.storage.core.HanabiEntry


/**
 * Created by Anur IjuoKaruKas on 2019/12/4
 *
 * 负责将 HanabiEntry 写入磁盘
 */
class FileHanabiEntry(val key: String, hanabiEntry: HanabiEntry) {

    companion object {
        // 理论上key 可以支持到很大，但是一个key 2g = = 玩呢？
        private const val DataSizeOffset = 0
        private const val DataSizeLength = 4
        private const val commandTypeOffset = DataSizeOffset + DataSizeLength
        private const val commandTypeLength = 1
        private const val operateTypeOffset = commandTypeOffset + commandTypeLength
        private const val operateTypeLength = 1
        private const val KeySizeOffset = operateTypeOffset + operateTypeLength
        private const val KeySizeLength = 4
        private const val KeyOffset = KeySizeOffset + KeySizeLength
        private const val sizeWithoutKey = KeyOffset
        private const val valueSizeLength = 4
        private const val sizeWithoutValue = sizeWithoutKey + valueSizeLength
    }

    var entry = hanabiEntry
    private var keyArr = key.toByteArray()
    private var valueArr = entry.value.toByteArray()
    private var expectedSize = sizeWithoutValue + keyArr.size + valueArr.size
    fun getExpectedSize(): Int = expectedSize

    /**
     * 更新内部元素
     */
    fun update(entry: HanabiEntry): Int {
        this.entry = entry
        valueArr = entry.value.toByteArray()
        expectedSize = sizeWithoutValue + keyArr.size + valueArr.size
        return expectedSize
    }

}