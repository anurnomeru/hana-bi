package com.anur.core.struct.coordinate;

import java.nio.ByteBuffer;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.AbstractTimedStruct;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
/**
 * Created by Anur IjuoKaruKas on 4/8/2019
 *
 * 告知客户端已经可以commit了
 */
public class Commiter extends AbstractTimedStruct {

    public static final int CanCommitGenerationOffset = TimestampOffset + TimestampLength;

    public static final int CanCommitGenerationLength = 8;

    public static final int CanCommitOffsetOffset = CanCommitGenerationOffset + CanCommitGenerationLength;

    public static final int CanCommitOffsetLength = 8;

    public static final int BaseMessageOverhead = CanCommitOffsetOffset + CanCommitOffsetLength;

    public Commiter(GenerationAndOffset canCommitGAO) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BaseMessageOverhead);
        init(byteBuffer, OperationTypeEnum.COMMIT);

        byteBuffer.putLong(canCommitGAO.getGeneration());
        byteBuffer.putLong(canCommitGAO.getOffset());

        byteBuffer.flip();
    }

    public Commiter(ByteBuffer byteBuffer) {
        this.buffer = byteBuffer;
    }

    public GenerationAndOffset getCanCommitGAO() {
        return new GenerationAndOffset(buffer.getLong(CanCommitGenerationOffset), buffer.getLong(CanCommitOffsetOffset));
    }

    @Override
    public void writeIntoChannel(Channel channel) {
        channel.write(Unpooled.wrappedBuffer(buffer));
    }

    @Override
    public int totalSize() {
        return size();
    }

    @Override
    public String toString() {
        return "Commiter{ GAO => " + getCanCommitGAO() + " }";
    }
}
