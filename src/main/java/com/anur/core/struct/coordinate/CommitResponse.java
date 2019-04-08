package com.anur.core.struct.coordinate;

import java.nio.ByteBuffer;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.AbstractTimedStruct;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 4/8/2019
 */
public class CommitResponse extends AbstractTimedStruct {

    public static final int CommitGenerationOffset = TimestampOffset + TimestampLength;

    public static final int CommitGenerationLength = 8;

    public static final int CommitOffsetOffset = CommitGenerationOffset + CommitGenerationLength;

    public static final int CommitOffsetLength = 8;

    public static final int BaseMessageOverhead = CommitOffsetOffset + CommitOffsetLength;

    public CommitResponse(GenerationAndOffset CommitGAO) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BaseMessageOverhead);
        init(byteBuffer, OperationTypeEnum.COMMIT_RESPONSE);

        byteBuffer.putLong(CommitGAO.getGeneration());
        byteBuffer.putLong(CommitGAO.getOffset());

        byteBuffer.flip();
    }

    public CommitResponse(ByteBuffer byteBuffer) {
        this.buffer = byteBuffer;
    }

    public GenerationAndOffset getCommitGAO() {
        return new GenerationAndOffset(buffer.getLong(CommitGenerationOffset), buffer.getLong(CommitOffsetOffset));
    }

    @Override
    public void writeIntoChannel(Channel channel) {
        channel.write(Unpooled.wrappedBuffer(buffer));
    }

    @Override
    public int totalSize() {
        return size();
    }
}
