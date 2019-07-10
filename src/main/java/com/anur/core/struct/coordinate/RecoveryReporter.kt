package com.anur.core.struct.coordinate

import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.AbstractTimedStruct
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/7/10
 *
 * 当选主成功后，从节点需要像主节点汇报自己最大的 offset
 */
class RecoveryReporter : AbstractTimedStruct {

    val LatestCommitGenerationOffset = TimestampOffset + TimestampLength

    val CanCommitGenerationLength = 8

    val LatestCommitOffsetOffset = LatestCommitGenerationOffset + CanCommitGenerationLength

    val CanCommitOffsetLength = 8

    val BaseMessageOverhead = LatestCommitOffsetOffset + CanCommitOffsetLength

    constructor(latestGAO: GenerationAndOffset) {
        val byteBuffer = ByteBuffer.allocate(BaseMessageOverhead)
        init(byteBuffer, OperationTypeEnum.FETCH)

        byteBuffer.putLong(latestGAO.generation)
        byteBuffer.putLong(latestGAO.offset)

        byteBuffer.flip()
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