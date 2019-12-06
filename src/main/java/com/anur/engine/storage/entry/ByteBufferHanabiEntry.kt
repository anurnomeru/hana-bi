package com.anur.engine.storage.entry

import com.anur.engine.api.constant.CommandTypeConst
import com.anur.engine.result.common.DataHandler
import com.anur.engine.storage.core.HanabiCommand
import com.anur.exception.UnSupportStorageTypeException
import java.nio.ByteBuffer


/**
 * Created by Anur IjuoKaruKas on 2019/12/4
 *
 * TODO hanabiCommand 和  ByteBufferHanabiEntry 共享底层 byteBuffer
 *
 * 负责将 HanabiEntry 写入磁盘
 *
 * 不包括 key 部分，则 HanabiEntry为以下部分组成：
 *
 *      1       +        1        +      4     + x
 *  commandType  + operationType  +  valueSize + value.
 */
class ByteBufferHanabiEntry(val byteBuffer: ByteBuffer) {

    /**
     * 对整个 ByteBufferHanabiEntry 大小的预估
     */
    val expectedSize: Int = byteBuffer.limit()

    var operateTypeSet = false

    /**
     *设置 operateType
     */
    fun setOperateType(operateType: OperateType) {
        byteBuffer.mark()
        byteBuffer.position(OperateTypeOffset)
        byteBuffer.put(operateType.byte)
        byteBuffer.reset()
        operateTypeSet = true
    }

    /**
     * 操作类型
     */
    fun getOperateType() = OperateType.map(byteBuffer.get(OperateTypeOffset))

    /**
     * STR操作还是LIST还是什么别的
     */
    fun getCommandType() = CommandTypeConst.map(byteBuffer.get(CommandTypeOffset))

    fun getValue(): String {
        byteBuffer.mark()
        byteBuffer.position(ValueSizeOffset)
        val mainParamSize = byteBuffer.getInt()

        val valueArray = ByteArray(mainParamSize)
        byteBuffer.get(valueArray)
        byteBuffer.reset()
        return String(valueArray)
    }

    companion object {

        val NONE: ByteBufferHanabiEntry

        init {
            val allocate = ByteBuffer.allocate(1)
            NONE = ByteBufferHanabiEntry(allocate)
        }

        // 理论上key 可以支持到很大，但是一个key 2g = = 玩呢？

        /**
         * 与 HanabiCommand 同义
         */
        const val CommandTypeOffset = 0
        const val CommandTypeLength = 1

        /**
         * 标记此值是否被删除
         */
        const val OperateTypeOffset = CommandTypeOffset + CommandTypeLength
        const val OperateTypeLength = 1

        /**
         * value 长度标识
         */
        const val ValueSizeOffset = OperateTypeOffset + OperateTypeLength
        const val ValueSizeLength = 4

        const val ValueOffset = ValueSizeOffset + ValueSizeLength

        /**
         * DISABLE 代表这个值被删掉了
         */
        enum class OperateType(val byte: Byte) {
            ENABLE(0),
            DISABLE(1);

            companion object {
                private val MAPPER = HashMap<Byte, OperateType>()

                init {
                    for (value in OperateType.values()) {
                        MAPPER[value.byte] = value
                    }
                }

                fun map(byte: Byte): OperateType {
                    return MAPPER[byte] ?: throw UnSupportStorageTypeException()
                }
            }
        }
    }
}