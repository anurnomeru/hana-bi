package com.anur.io.coordinate.client;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.core.client.ReconnectableClient;
import com.anur.io.core.coder.CoordinateDecoder;
import com.anur.io.core.coder.CoordinateEncoder;
import com.anur.io.core.handle.ByteBufferMsgConsumerHandler;
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
    private BiConsumer<ChannelHandlerContext, ByteBuffer> msgConsumer;

    public CoordinateClient(String serverName, String host, int port, ShutDownHooker shutDownHooker,
        BiConsumer<ChannelHandlerContext, ByteBuffer> msgConsumer) {
        super(serverName, host, port, shutDownHooker);
        this.msgConsumer = msgConsumer;
    }

    @Override
    public ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline) {
        return channelPipeline.addLast(new CoordinateDecoder())
                              .addLast(new CoordinateEncoder())
                              .addLast(new han())
                              .addLast(new ByteBufferMsgConsumerHandler(msgConsumer));
    }

    @Override
    public void howToRestart() {
        new CoordinateClient(this.serverName, this.host, this.port, this.shutDownHooker, this.msgConsumer).start();
    }

    static class han extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);

            String s = "测试能否正确编解码";
            byte[] bytes = s.getBytes();
            ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
            byteBuffer.put(bytes);
            ctx.writeAndFlush(byteBuffer);
        }
    }
}
