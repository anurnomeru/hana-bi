package com.anur.io.elect.client;

import java.util.function.BiConsumer;
import com.anur.core.util.ChannelManager.ChannelType;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.core.Client;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LineBasedFrameDecoder;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 *
 * 负责连接其他的服务器
 */
public class ElectClient extends Client {

    public ElectClient(String serverName, String host, int port, BiConsumer<ChannelHandlerContext, String> msgConsumer,
        ShutDownHooker shutDownHooker) {
        super(serverName, host, port, msgConsumer, shutDownHooker);
    }

    @Override
    public void channelPipelineConsumer(ChannelPipeline channelPipeline) {
        channelPipeline
            .addLast("LineBasedFrameDecoder", new LineBasedFrameDecoder(Integer.MAX_VALUE))
            .addLast("ClientChannelHandler", new ClientChannelHandler(ChannelType.ELECT, serverName))
            .addLast("ClientElectHandler", new ClientElectHandler(msgConsumer));
    }

    @Override
    public void howToRestart() {
        new ElectClient(this.serverName, this.host, this.port, this.msgConsumer, this.shutDownHooker).start();
    }
}
