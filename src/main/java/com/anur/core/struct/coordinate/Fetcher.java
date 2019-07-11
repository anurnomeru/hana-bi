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
 * 用于向协调 Leader 拉取消息，并告知自己的提交进度
 */
public class Fetcher extends AbstractTimedStruct {

    public static final int FetchGenerationOffset = TimestampOffset + TimestampLength;

    public static final int FetchGenerationLength = 8;

    public static final int FetchOffsetOffset = FetchGenerationOffset + FetchGenerationLength;

    public static final int FetchOffsetLength = 8;

    public static final int BaseMessageOverhead = FetchOffsetOffset + FetchOffsetLength;

    public Fetcher(GenerationAndOffset fetchGAO) {
        init(BaseMessageOverhead, OperationTypeEnum.FETCH, buffer -> {
            buffer.putLong(fetchGAO.getGeneration());
            buffer.putLong(fetchGAO.getOffset());
        });
    }

    public Fetcher(ByteBuffer byteBuffer) {
        this.buffer = byteBuffer;
    }

    public GenerationAndOffset getFetchGAO() {
        return new GenerationAndOffset(buffer.getLong(FetchGenerationOffset), buffer.getLong(FetchOffsetOffset));
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
        return "Fetcher{ GAO => " + getFetchGAO().toString() + "}";
    }
}
