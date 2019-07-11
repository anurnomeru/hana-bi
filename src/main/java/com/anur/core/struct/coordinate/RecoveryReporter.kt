package com.anur.core.struct.coordinate

import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.AbstractTimedStruct
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import java.nio.ByteBuffer
import java.util.function.Consumer

/**
 * Created by Anur IjuoKaruKas on 2019/7/10
 *
 * 当选主成功后，从节点需要像主节点汇报自己最大的 offset
 */
class RecoveryReporter : AbstractTimedStruct {

    private val LatestCommitGenerationOffset = TimestampOffset + TimestampLength

    private val CanCommitGenerationLength = 8

    private val LatestCommitOffsetOffset = LatestCommitGenerationOffset + CanCommitGenerationLength

    private val CanCommitOffsetLength = 8

    private val BaseMessageOverhead: Int = LatestCommitOffsetOffset + CanCommitOffsetLength

    constructor(latestGAO: GenerationAndOffset) {
        init(BaseMessageOverhead, OperationTypeEnum.RECOVERY, Consumer {
            byteBuffer.putLong(latestGAO.generation)
            byteBuffer.putLong(latestGAO.offset)
        })
    }

    constructor(byteBuffer: ByteBuffer) {
        this.buffer = byteBuffer
    }

    fun getLatestGAO(): GenerationAndOffset {
        return GenerationAndOffset(buffer.getLong(LatestCommitGenerationOffset), buffer.getLong(LatestCommitOffsetOffset))
    }


    override fun writeIntoChannel(channel: Channel) {
        channel.write(Unpooled.wrappedBuffer(buffer))
    }

    override fun totalSize(): Int {
        return size()
    }

    override fun toString(): String {
        return "Commiter{ GAO => ${getLatestGAO()} }"
    }

}