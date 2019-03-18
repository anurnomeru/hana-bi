package com.anur.io.coordinate.server;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
    private final BiConsumer<ChannelHandlerContext, ByteBuffer> msgConsumer;

    private final Consumer<ChannelPipeline> channelPipelineConsumer;

    public CoordinateServer(int port, ShutDownHooker shutDownHooker, BiConsumer<ChannelHandlerContext, ByteBuffer> msgConsumer, Consumer<ChannelPipeline> channelPipelineConsumer) {
        super(port, shutDownHooker);
        this.msgConsumer = msgConsumer;
        this.channelPipelineConsumer = channelPipelineConsumer;
    }

    @Override
    public ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline) {
        channelPipeline.addLast(new CoordinateDecoder())
                       .addLast(new ByteBufferMsgConsumerHandler(msgConsumer));
        channelPipelineConsumer.accept(channelPipeline);
        return channelPipeline;
    }
}
