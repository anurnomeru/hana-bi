package com.anur.io.coordinate.server;

import java.util.function.BiConsumer;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.core.Server;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;

/**
 * Created by Anur IjuoKaruKas on 2/12/2019
 *
 * 供其他端连接的套接字服务端入口，此端口负责集群内协调
 */
public class CoordinateServer extends Server {

    public CoordinateServer(int port, BiConsumer<ChannelHandlerContext, String> msgConsumer, ShutDownHooker shutDownHooker) {
        super(port, msgConsumer, shutDownHooker);
    }

    @Override
    public ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline) {
        return channelPipeline;
    }
}
