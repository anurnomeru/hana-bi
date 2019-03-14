package com.anur.io.core.coder;

import java.nio.ByteBuffer;
import com.anur.config.LogConfigHelper;
import com.anur.core.util.ByteBufferUtil;
import com.anur.core.util.Crc32;
import com.anur.exception.HanabiException;
import com.anur.io.store.common.OperationConstant;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by Anur IjuoKaruKas on 2019/3/14
 */
public class CoordinateEncoder extends MessageToByteEncoder {

    private static int CrcLength = 4;

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Object o, ByteBuf byteBuf) throws Exception {
        ByteBuffer byteBuffer = (ByteBuffer) o;
        byteBuffer.rewind();
        int limit = byteBuffer.limit();
        if (limit > LogConfigHelper.getMaxLogMessageSize()) {
            throw new HanabiException("集群协调消息的大小不能大于" + LogConfigHelper.getMaxLogMessageSize());
        }

        byteBuf.writeInt(CrcLength + limit);
        byteBuf.writeInt(getUnsignedInt(Crc32.crc32(byteBuffer.array())));
        byteBuf.writeBytes(byteBuffer);
    }

    public int getUnsignedInt(long v) {
        return (int) (v & 0xffffffffL);
    }
}
