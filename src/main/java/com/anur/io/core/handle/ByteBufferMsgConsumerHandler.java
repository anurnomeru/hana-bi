package com.anur.io.core.handle;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Created by Anur IjuoKaruKas on 2019/3/14
 */
public class ByteBufferMsgConsumerHandler extends SimpleChannelInboundHandler {

    /**
     * 将如何消费消息的权利交给上级，将业务处理从Handler中隔离
     */
    private BiConsumer<ChannelHandlerContext, ByteBuffer> msgConsumer;

    public ByteBufferMsgConsumerHandler(BiConsumer<ChannelHandlerContext, ByteBuffer> msgConsumer) {
        this.msgConsumer = msgConsumer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) {
        ByteBuffer byteBuffer = ((ByteBuffer) o);
        msgConsumer.accept(channelHandlerContext, byteBuffer);
    }
}
