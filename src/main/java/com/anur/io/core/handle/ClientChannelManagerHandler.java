package com.anur.io.core.handle;

import com.anur.util.ChannelManager;
import com.anur.util.ChannelManager.ChannelType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Created by Anur IjuoKaruKas on 2/2/2019
 *
 * 管理channel下各个连接
 */
public class ClientChannelManagerHandler extends ChannelInboundHandlerAdapter {

    private ChannelType channelType;

    private String serverName;

    public ClientChannelManagerHandler(ChannelType channelType, String serverName) {
        this.channelType = channelType;
        this.serverName = serverName;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        ChannelManager.getInstance(channelType)
                      .register(serverName, ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        ChannelManager.getInstance(channelType)
                      .unRegister(serverName);
    }
}
