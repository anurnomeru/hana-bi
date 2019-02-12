package com.anur.io.coordinate.client;

import java.util.function.BiConsumer;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.core.Client;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;

/**
 * Created by Anur IjuoKaruKas on 2/12/2019
 *
 * 负责连接leader，来进行集群内协调
 */
public class CoordinateClient extends Client {

    public CoordinateClient(String serverName, String host, int port, BiConsumer<ChannelHandlerContext, String> msgConsumer,
        ShutDownHooker shutDownHooker) {
        super(serverName, host, port, msgConsumer, shutDownHooker);
    }

    @Override
    public ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline) {
        return channelPipeline;
    }

    @Override
    public void howToRestart() {
        new CoordinateClient(this.serverName, this.host, this.port, this.msgConsumer, this.shutDownHooker).start();
    }
}
