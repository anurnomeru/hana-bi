package com.anur.io.core.coder;

import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.LogConfigHelper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Created by Anur IjuoKaruKas on 2019/3/14
 */
public class CoordinateDecoder extends ByteToMessageDecoder {

    private Logger logger = LoggerFactory.getLogger(CoordinateDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> list) {
        ByteBuffer o = decode(ctx, buffer);
        if (o != null) {
            list.add(o);
        }
    }

    /**
     * 4位 长度  + 消息内容
     */
    private ByteBuffer decode(ChannelHandlerContext ctx, ByteBuf buffer) {
        buffer.markReaderIndex();
        int maybeLength = buffer.readInt();

        logger.trace("maybeLength => " + maybeLength);

        if (maybeLength > LogConfigHelper.getMaxLogMessageSize()) {
            buffer.discardReadBytes();
            return null;
        }

        int remain = buffer.readableBytes();

        logger.trace("remain => " + remain);

        if (remain < maybeLength) {
            logger.trace("消息解析异常，remain {} 但是 maybeLength {}", remain, maybeLength);
            buffer.resetReaderIndex();
            return null;
        } else {
            buffer.markReaderIndex();
            byte[] bytes = new byte[maybeLength];
            buffer.readBytes(bytes);

            ByteBuffer result = ByteBuffer.allocate(maybeLength);
            result.put(bytes);
            result.rewind();
            return result;
        }
    }
}
