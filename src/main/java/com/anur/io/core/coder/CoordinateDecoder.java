package com.anur.io.core.coder;

import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.AbstractStruct;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Created by Anur IjuoKaruKas on 2019/3/14
 */
public class CoordinateDecoder extends ByteToMessageDecoder {

    private static final int LengthInBytes = 4;

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
        logger.trace("before read index:" + buffer.readerIndex());
        int maybeLength = buffer.readInt();
        logger.trace("after read index:" + buffer.readerIndex());

        logger.trace("maybeLength => " + maybeLength);

        int remain = buffer.readableBytes();
        logger.trace("remain => " + remain);

        if (remain < maybeLength) {
            logger.trace("消息解析异常，remain {} 但是 maybeLength {}", remain, maybeLength);
            buffer.resetReaderIndex();
            logger.trace("after reset index:" + buffer.readerIndex());
            return null;
        } else {

            buffer.markReaderIndex();
            byte[] bytes = new byte[maybeLength];
            buffer.readBytes(bytes);

            ByteBuffer result = ByteBuffer.allocate(maybeLength);
            result.put(bytes);
            result.rewind();
            return result;

            // todo 此处还有bug！！
/*            int sign = buffer.getInt(AbstractStruct.TypeOffset + 4);
            OperationTypeEnum typeEnum = OperationTypeEnum.parseByByteSign(sign);

            // 标识此index后的已经读过了
            buffer.readerIndex(LengthInBytes + maybeLength);

            // 第一个字节是长度，和业务无关
            return buffer.nioBuffer(LengthInBytes, maybeLength);*/
        }
    }
}
