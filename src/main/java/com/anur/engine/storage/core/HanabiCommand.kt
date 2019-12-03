package com.anur.engine.storage.core

import com.anur.engine.api.constant.StorageTypeConst
import com.anur.engine.api.constant.TransactionTypeConst
import com.anur.exception.HanabiException
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/9/17
 *
 * 对应一个最基础最基础的操作
 */
class HanabiCommand(val content: ByteBuffer) {

    companion object {
        private const val TrxIdOffset = 0
        private const val TrxIdLength = 8
        private const val TransactionOffset = TrxIdOffset + TrxIdLength
        private const val TransactionLength = 1
        private const val TypeOffset = TransactionOffset + TransactionLength
        private const val TypeLength = 1
        private const val ApiOffset = TypeOffset + TypeLength
        private const val ApiLength = 1

        /**
         * 此值可传多参数，参数个数int + length + value 组成
         */
        private const val ValueOffset = ApiOffset + ApiLength

        /**
         * 表明参数长度，四个字节
         */
        private const val ValuesSizeLength = 4

        fun generator(trxId: Long, transaction: TransactionTypeConst, type: StorageTypeConst, api: Byte, vararg values: String = arrayOf("")): HanabiCommand {
            if (values.isEmpty()) {
                throw HanabiException("不允许生成值为空数组的命令！至少要传一个含有空字符串的数组")
            }

            val valuesSizeLengthTotal = values.size * ValuesSizeLength
            val valuesByteArr = values.map { it?.toByteArray() }
            val bb = ByteBuffer.allocate(
                    ValueOffset
                            + valuesSizeLengthTotal
                            + valuesByteArr.map { it?.size ?: 0 }.reduce(operation = { i1, i2 -> i1 + i2 }))
            bb.putLong(trxId)
            bb.put(transaction.byte)
            bb.put(type.byte)
            bb.put(api)
            valuesByteArr.forEach {
                it.also { arr ->
                    if (arr==null) {
                        bb.putInt(0)
                    }else{
                        bb.putInt(arr.size)
                        bb.put(arr)
                    }
                }
            }
            bb.flip()
            return HanabiCommand(bb)
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
     * 是否开启了（长）事务
     */
    fun getTransactionType(): Byte {
        return content.get(TransactionOffset)
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
    fun getValues(): MutableList<String> {
        val list = mutableListOf<String>()
        content.position(ValueOffset)
        while (content.position() < contentLength) {
            val param = ByteArray(content.getInt())
            content.get(param)
            list.add(String(param))
        }
        return list
    }

    override fun toString(): String {
        return "HanabiEntry{" +
                "trxId='" + getTrxId() + '\'' +
                ", type='" + getType() + '\'' +
                ", api='" + getApi() + '\'' +
                ", value='" + getValues() + '\'' +
                "}"
    }
}