package com.anur.engine.storage.entry


/**
 * Created by Anur IjuoKaruKas on 2019/12/5
 */
class FileHanabiEntry {

    companion object {
        const val SizeOffset = 0
        const val SizeLength = 4

        const val KeySizeOffset = SizeOffset + SizeLength
        const val KeySizeLength = 4

        const val KeyOffset = KeySizeOffset + KeySizeLength

        const val minFileHanabiEntryOverHead = KeyOffset

        /**
         * 对整个 ByteBufferHanabiEntry 大小的预估
         */
        fun getExpectedSize(key: String, entry: ByteBufferHanabiEntry): Int = minFileHanabiEntryOverHead + key.toByteArray().size + entry.getExpectedSize()
    }


}