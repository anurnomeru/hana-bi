package com.anur.io.coordinate.server;

import java.net.SocketAddress;
import com.anur.core.util.ChannelManager.ChannelType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Created by Anur IjuoKaruKas on 2/13/2019
 */
public class CoordinateServerChannelManagerHandler extends ChannelInboundHandlerAdapter {

    private ChannelType channelType;

    public CoordinateServerChannelManagerHandler(ChannelType channelType) {
        this.channelType = channelType;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        SocketAddress socketAddress = ctx.channel()
                                         .remoteAddress();
        System.out.println(socketAddress.toString());
        System.out.println("123123123123123123");

        //        ChannelManager.getInstance(channelType)
        //                      .register(serverName, ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        //        ChannelManager.getInstance(channelType)
        //                      .unRegister(serverName);
    }
}
