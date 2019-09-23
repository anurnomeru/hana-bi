package com.anur.io.engine.storage.core

import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/9/17
 *
 * 对应一个最基础最基础的操作
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

    /**
     * 事务 id
     */
    fun getTrxId(): Long {
        return content.getLong(TrxIdOffset)
    }

    /**
     * 操作类型，目前仅支持String类操作，第一版不要做那么复杂
     */
    fun getType(): Byte {
        return content.get(TypeOffset)
    }

    /**
     * 操作具体的api是哪个，比如增删改查之类的
     */
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