package com.anur.io.core.coder;

import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        logger.trace("before read index:" + buffer.readerIndex());
        int maybeLength = buffer.readInt();
        logger.trace("after read index:" + buffer.readerIndex());

        logger.trace("maybeLength => " + maybeLength);

        int remain = buffer.readableBytes();
        logger.trace("remain => " + remain);

        if (remain == 0) {
            System.out.println("----");
        }

        if (remain < maybeLength) {
            logger.trace("消息解析异常，remain {} 但是 maybeLength {}", remain, maybeLength);
            buffer.resetReaderIndex();
            logger.trace("after reset index:" + buffer.readerIndex());
            return null;
        } else {
            // ver1.0 通过ByteBuffer直接去读
            ByteBuffer resultOne = ByteBuffer.allocate(maybeLength);
            buffer.readBytes(resultOne);
            resultOne.rewind();

            //            // ver2.0 通过ByteBuf原生提供的API
            //            // 标识此index后的已经读过了
            //            buffer.resetReaderIndex();
            //            buffer.readerIndex(LengthInBytes + maybeLength);
            //
            //            //             第一个字节是长度，和业务无关
            //            ByteBuffer resultTwo = buffer.nioBuffer(LengthInBytes, maybeLength);
            //
            //            byte[] bytesFromResultOne = new byte[maybeLength];
            //            resultOne.get(bytesFromResultOne);
            //
            //            byte[] bytesFromResultTwo = new byte[maybeLength];
            //            resultTwo.get(bytesFromResultTwo);
            //
            //            if (!Arrays.toString(bytesFromResultOne)
            //                       .equals(Arrays.toString(bytesFromResultTwo))) {
            //                System.out.println();
            //            }
            //            resultTwo.rewind();
            //            resultTwo.rewind();

            return resultOne;
        }
    }
}
