package com.anur.io.coordinate.server;

import java.util.function.BiConsumer;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.core.handle.MsgConsumeHandler;
import com.anur.io.core.server.Server;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LineBasedFrameDecoder;

/**
 * Created by Anur IjuoKaruKas on 2/12/2019
 *
 * 供其他端连接的套接字服务端入口，此端口负责集群内协调
 */
public class CoordinateServer extends Server {

    /**
     * 将如何消费消息的权利交给上级，将业务处理从Server中剥离
     */
    protected BiConsumer<ChannelHandlerContext, String> msgConsumer;

    public CoordinateServer(int port, ShutDownHooker shutDownHooker, BiConsumer<ChannelHandlerContext, String> msgConsumer) {
        super(port, shutDownHooker);
        this.msgConsumer = msgConsumer;
    }

    @Override
    public ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline) {
        return channelPipeline.addLast(new LineBasedFrameDecoder(Integer.MAX_VALUE))
                              .addLast(new MsgConsumeHandler(msgConsumer));
    }
}
