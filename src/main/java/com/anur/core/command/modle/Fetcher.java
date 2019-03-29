package com.anur.core.command.modle;

import java.nio.ByteBuffer;
import com.anur.core.command.common.AbstractCommand;
import com.anur.core.command.common.OperationTypeEnum;
import com.anur.core.elect.model.GenerationAndOffset;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 2019/3/29
 *
 * 用于向协调 Leader 拉取消息
 */
public class Fetcher extends AbstractCommand {

    public static final int TimestampOffset = TypeOffset + TypeLength;

    public static final int TimestampLength = 8;

    public static final int GenerationOffset = TimestampOffset + TimestampLength;

    public static final int GenerationLength = 8;

    public static final int OffsetOffset = GenerationOffset + GenerationLength;

    public static final int OffsetLength = 8;

    public static final int BaseMessageOverhead = OffsetOffset + OffsetLength;

    public Fetcher(GenerationAndOffset GAO) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BaseMessageOverhead);
        byteBuffer.position(TypeOffset);
        byteBuffer.putInt(OperationTypeEnum.FETCH.byteSign);
        byteBuffer.putLong(System.currentTimeMillis());
        byteBuffer.putLong(GAO.getGeneration());
        byteBuffer.putLong(GAO.getOffset());

        this.buffer = byteBuffer;
        long crc = computeChecksum();

        byteBuffer.position(0);
        byteBuffer.putInt((int) crc);

        byteBuffer.rewind();
    }

    public Fetcher(ByteBuffer byteBuffer) {
        this.buffer = byteBuffer;
        ensureValid();
    }

    public GenerationAndOffset getGAO() {
        return new GenerationAndOffset(buffer.getLong(GenerationOffset), buffer.getLong(OffsetOffset));
    }

    @Override
    public void writeIntoChannel(Channel channel) {

    }

    @Override
    public int totalSize() {
        return 0;
    }
}
