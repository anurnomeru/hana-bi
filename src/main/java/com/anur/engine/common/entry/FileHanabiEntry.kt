package com.anur.engine.common.entry


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
        fun getExpectedSizeOverHead(key: String): Int = minFileHanabiEntryOverHead + key.toByteArray().size
    }
}