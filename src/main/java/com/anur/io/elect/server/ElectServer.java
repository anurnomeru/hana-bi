package com.anur.io.elect.server;

import java.util.function.BiConsumer;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.elect.core.Server;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LineBasedFrameDecoder;

/**
 * Created by Anur IjuoKaruKas on 2019/1/18
 *
 * 供其他端连接的套接字服务端入口，ElectServer与选举逻辑无关，它只控制socket服务的开启和关闭
 */
public class ElectServer extends Server {

    public ElectServer(int port, BiConsumer<ChannelHandlerContext, String> msgConsumer, ShutDownHooker shutDownHooker) {
        super(port, msgConsumer, shutDownHooker);
    }

    @Override
    public void channelPipelineConsumer(ChannelPipeline channelPipeline) {
        channelPipeline.addLast(new LineBasedFrameDecoder(Integer.MAX_VALUE))
                       .addLast(new ServerElectHandler(msgConsumer));
    }
}
