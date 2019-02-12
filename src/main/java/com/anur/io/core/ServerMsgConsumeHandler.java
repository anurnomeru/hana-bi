package com.anur.io.core;

import java.nio.charset.Charset;
import java.util.function.BiConsumer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Created by Anur IjuoKaruKas on 2019/1/18
 *
 * 服务器的选举信息接收处理器，这个需要一直启动着，等待其他节点发送选票过来，
 */
public class ServerMsgConsumeHandler extends SimpleChannelInboundHandler {

    /**
     * 将如何消费消息的权利交给上级，将业务处理从Handler中隔离
     */
    private BiConsumer<ChannelHandlerContext, String> msgConsumer;

    public ServerMsgConsumeHandler(BiConsumer<ChannelHandlerContext, String> msgConsumer) {
        this.msgConsumer = msgConsumer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) {
        String str = ((ByteBuf) o).toString(Charset.defaultCharset());
        msgConsumer.accept(channelHandlerContext, str);
    }
}
