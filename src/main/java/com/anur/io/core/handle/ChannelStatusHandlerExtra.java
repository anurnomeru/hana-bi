package com.anur.io.core.handle;

import java.util.function.Consumer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Created by Anur IjuoKaruKas on 2/13/2019
 */
public class ChannelStatusHandlerExtra extends ChannelInboundHandlerAdapter {

    private Consumer<ChannelHandlerContext> channelActiveConsumer;

    private Consumer<ChannelHandlerContext> channelInactiveConsumer;

    public ChannelStatusHandlerExtra(Consumer<ChannelHandlerContext> channelActiveConsumer, Consumer<ChannelHandlerContext> channelInactiveConsumer) {
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
