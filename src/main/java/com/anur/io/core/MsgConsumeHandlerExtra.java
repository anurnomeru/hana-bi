package com.anur.io.core;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by Anur IjuoKaruKas on 2/13/2019
 */
public class MsgConsumeHandlerExtra extends MsgConsumeHandler {

    private Consumer<ChannelHandlerContext> channelActiveConsumer;

    private Consumer<ChannelHandlerContext> channelInactiveConsumer;

    public MsgConsumeHandlerExtra(BiConsumer<ChannelHandlerContext, String> msgConsumer, Consumer<ChannelHandlerContext> channelActiveConsumer,
        Consumer<ChannelHandlerContext> channelInactiveConsumer) {
        super(msgConsumer);
        this.channelActiveConsumer = channelActiveConsumer;
        this.channelInactiveConsumer = channelInactiveConsumer;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        channelActiveConsumer.accept(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        channelInactiveConsumer.accept(ctx);
    }
}
