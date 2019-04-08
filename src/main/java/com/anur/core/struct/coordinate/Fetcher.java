package com.anur.core.struct.coordinate;

import java.nio.ByteBuffer;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.AbstractTimedStruct;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 2019/3/29
 *
 * 用于向协调 Leader 拉取消息
 */
public class Fetcher extends AbstractTimedStruct {

    public static final int GenerationOffset = TimestampOffset + TimestampLength;

    public static final int GenerationLength = 8;

    public static final int OffsetOffset = GenerationOffset + GenerationLength;

    public static final int OffsetLength = 8;

    public static final int BaseMessageOverhead = OffsetOffset + OffsetLength;

    public Fetcher(GenerationAndOffset GAO) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BaseMessageOverhead);
        init(byteBuffer, OperationTypeEnum.FETCH);

        byteBuffer.putLong(GAO.getGeneration());
        byteBuffer.putLong(GAO.getOffset());

        byteBuffer.flip();
    }

    public Fetcher(ByteBuffer byteBuffer) {
        this.buffer = byteBuffer;
    }

    public GenerationAndOffset getGAO() {
        return new GenerationAndOffset(buffer.getLong(GenerationOffset), buffer.getLong(OffsetOffset));
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
        return "Fetcher{ GAO => " + getGAO().toString() + "}";
    }
}
