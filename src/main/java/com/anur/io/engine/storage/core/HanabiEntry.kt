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
        private const val TypeLength = 1
        private const val ApiOffset = TypeOffset + TypeLength
        private const val ApiLength = 1
        private const val ValueOffset = ApiOffset + ApiLength

        fun generator(trxId: Long, type: Byte, api: Byte, value: String): HanabiEntry {
            val valueArray = value.toByteArray()
            val bb = ByteBuffer.allocate(ValueOffset + valueArray.size)
            bb.putLong(trxId)
            bb.put(type)
            bb.put(api)
            bb.put(valueArray)
            bb.flip()
            return HanabiEntry(bb)
        }
    }

    val contentLength = content.limit()

    fun getTrxId(): Long {
        return content.getLong(TrxIdOffset)
    }

    fun getType(): Byte {
        return content.get(TypeOffset)
    }

    fun getApi(): Byte {
        return content.get(ApiOffset)
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