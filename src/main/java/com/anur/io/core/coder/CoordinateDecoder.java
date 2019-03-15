package com.anur.io.core.coder;

import java.nio.ByteBuffer;
import java.util.List;
import com.anur.config.LogConfigHelper;
import com.anur.core.util.Crc32;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Created by Anur IjuoKaruKas on 2019/3/14
 */
public class CoordinateDecoder extends ByteToMessageDecoder {

    private static int CrcLength = 4;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> list) throws Exception {
        ByteBuffer o = decode(ctx, buffer);
        if (o != null) {
            list.add(o);
        }
    }

    /**
     * 4位 长度 + 4位 CRC + msg
     */
    private ByteBuffer decode(ChannelHandlerContext ctx, ByteBuf buffer) {
        buffer.markReaderIndex();
        int maybeLength = buffer.readInt();

        if (maybeLength > LogConfigHelper.getMaxLogMessageSize()) {
            buffer.discardReadBytes();
            return null;
        }

        int remain = buffer.readableBytes();

        if (remain < maybeLength) {
            buffer.resetReaderIndex();
            return null;
        } else {
            buffer.markReaderIndex();

            int crc = buffer.readInt();
            byte[] bytes = new byte[maybeLength - CrcLength];

            buffer.readBytes(bytes);
            int crc32Compute = toUnsignedInt(Crc32.crc32(bytes));

            if (crc != crc32Compute) {
                buffer.resetReaderIndex();
                buffer.discardReadBytes();
                return null;
            } else {
                ByteBuffer result = ByteBuffer.allocate(maybeLength - CrcLength);
                result.put(bytes);
                result.rewind();
                return result;
            }
        }
    }

    private int toUnsignedInt(long v) {
        return (int) (v & 0xffffffffL);
    }
}
