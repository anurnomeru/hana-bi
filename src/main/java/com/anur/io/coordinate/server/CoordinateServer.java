package com.anur.io.coordinate.server;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.core.coder.CoordinateDecoder;
import com.anur.io.core.coder.CoordinateEncoder;
import com.anur.io.core.handle.ByteBufferMsgConsumerHandler;
import com.anur.io.core.handle.StrMsgConsumeHandler;
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
    protected BiConsumer<ChannelHandlerContext, ByteBuffer> msgConsumer;

    public CoordinateServer(int port, ShutDownHooker shutDownHooker, BiConsumer<ChannelHandlerContext, ByteBuffer> msgConsumer) {
        super(port, shutDownHooker);
        this.msgConsumer = msgConsumer;
    }

    @Override
    public ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline) {
        return channelPipeline.addLast(new CoordinateDecoder())
                              .addLast(new CoordinateEncoder())
                              .addLast(new ByteBufferMsgConsumerHandler(msgConsumer));
    }
}
