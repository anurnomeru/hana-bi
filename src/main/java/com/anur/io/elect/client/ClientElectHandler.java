package com.anur.io.elect.client;

import java.nio.charset.Charset;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 *
 * 当要成为领导时，需要去连接其他的 ServerElectHandler，并去发送各种选举相关的消息
 */
public class ClientElectHandler extends SimpleChannelInboundHandler {

    private Logger logger = LoggerFactory.getLogger(ClientElectHandler.class);

    /**
     * 将如何消费消息的权利交给上级，将业务处理从Handler中隔离
     */
    private BiConsumer<ChannelHandlerContext, String> msgConsumer;

    public ClientElectHandler(BiConsumer<ChannelHandlerContext, String> msgConsumer) {
        this.msgConsumer = msgConsumer;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
        String str = ((ByteBuf) o).toString(Charset.defaultCharset());
        msgConsumer.accept(channelHandlerContext, str);
    }
}