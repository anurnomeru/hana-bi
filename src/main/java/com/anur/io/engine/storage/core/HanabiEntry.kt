package com.anur.io.engine.storage.core

import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/9/17
 */
class HanabiEntry(val content: ByteBuffer) {

    companion object {
        private const val TrxIdOffset = 0
        private const val TrxIdLength = 8
        private const val TypeOffset = TrxIdOffset + TrxIdLength
        private const val TypeLength = 2
        private const val ApiOffset = TypeOffset + TypeLength
        private const val ApiLength = 2
        private const val ValueOffset = ApiOffset + ApiLength

        fun generator(trxId: Long, type: Short, api: Short, value: String): HanabiEntry {
            val valueArray = value.toByteArray()
            val bb = ByteBuffer.allocate(ValueOffset + valueArray.size)
            bb.putLong(trxId)
            bb.putShort(type)
            bb.putShort(api)
            bb.put(valueArray)
            bb.flip()
            return HanabiEntry(bb)
        }
    }

    val contentLength = content.limit()

    fun getTrxId(): Long {
        return content.getLong(TrxIdOffset)
    }

    fun getType(): Short {
        return content.getShort(TypeOffset)
    }

    fun getApi(): Short {
        return content.getShort(ApiOffset)
    }

    @Synchronized
    fun getValue(): String {
        val valueLength = contentLength - ValueOffset
        val bs = ByteArray(valueLength)
        content.position(ValueOffset)
        content.get(bs)
        content.position(0)
        return String(bs)
    }

    override fun toString(): String {
        return "HanabiEntry{" +
            "trxId='" + getTrxId() + '\'' +
            ", type='" + getType() + '\'' +
            ", api='" + getApi() + '\'' +
            ", value='" + getValue() + '\'' +
            "}"
    }
}