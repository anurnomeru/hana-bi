package com.anur.io.coordinate.client;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.core.client.ReconnectableClient;
import com.anur.io.core.coder.CoordinateDecoder;
import com.anur.io.core.coder.CoordinateEncoder;
import com.anur.io.core.handle.ByteBufferMsgConsumerHandler;
import com.anur.io.core.handle.ErrorHandler;
import com.anur.io.store.common.Operation;
import com.anur.io.store.common.OperationTypeEnum;
import com.anur.io.store.operationset.ByteBufferOperationSet;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;

/**
 * Created by Anur IjuoKaruKas on 2/12/2019
 *
 * 负责连接leader，来进行集群内协调
 */
public class CoordinateClient extends ReconnectableClient {

    /**
     * 将如何消费消息的权利交给上级，将业务处理从Client中剥离
     */
    private final BiConsumer<ChannelHandlerContext, ByteBuffer> msgConsumer;

    /**
     * 需要添加哪些额外的 ChannelPipeline
     */
    private final Consumer<ChannelPipeline> channelPipelineConsumer;

    public CoordinateClient(String serverName, String host, int port, ShutDownHooker shutDownHooker,
        BiConsumer<ChannelHandlerContext, ByteBuffer> msgConsumer, Consumer<ChannelPipeline> channelPipelineConsumer) {
        super(serverName, host, port, shutDownHooker);
        this.msgConsumer = msgConsumer;
        this.channelPipelineConsumer = channelPipelineConsumer;
    }

    @Override
    public ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline) {
        channelPipeline.addLast(new CoordinateDecoder())
                       .addLast(new ByteBufferMsgConsumerHandler(msgConsumer))
                       .addLast(new ErrorHandler());

        channelPipelineConsumer.accept(channelPipeline);
        return channelPipeline;
    }

    @Override
    public void howToRestart() {
        new CoordinateClient(this.serverName, this.host, this.port, this.shutDownHooker, this.msgConsumer, this.channelPipelineConsumer).start();
    }
}
