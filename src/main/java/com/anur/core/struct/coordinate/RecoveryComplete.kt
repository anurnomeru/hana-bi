package com.anur.core.struct.coordinate

import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.AbstractTimedStruct
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import java.nio.ByteBuffer
import java.util.function.Consumer

/**
 * Created by Anur IjuoKaruKas on 2019/7/11
 *
 * 集群恢复完毕后，leader 告诉 从节点，其世代最大 GAO 为多少
 */
class RecoveryComplete : AbstractTimedStruct {

    private val CommitedGenerationLength = 8

    private val CommitedOffsetOffset = OriginMessageOverhead + CommitedGenerationLength

    private val CommitedOffsetLength = 8

    private val BaseMessageOverhead: Int = CommitedOffsetOffset + CommitedOffsetLength

    constructor(latestGAO: GenerationAndOffset) {
        init(BaseMessageOverhead, OperationTypeEnum.RECOVERY) {
            byteBuffer.putLong(latestGAO.generation)
            byteBuffer.putLong(latestGAO.offset)
        }
    }

    constructor(byteBuffer: ByteBuffer) {
        this.buffer = byteBuffer
    }

    fun getCommited(): GenerationAndOffset {
        return GenerationAndOffset(buffer.getLong(OriginMessageOverhead), buffer.getLong(CommitedOffsetOffset))
    }


    override fun writeIntoChannel(channel: Channel) {
        channel.write(Unpooled.wrappedBuffer(buffer))
    }

    override fun totalSize(): Int {
        return size()
    }

    override fun toString(): String {
        return "RecoveryComplete { GAO => ${getCommited()} }"
    }
}