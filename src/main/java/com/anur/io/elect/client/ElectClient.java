package com.anur.io.elect.client;

import java.util.function.BiConsumer;
import com.anur.core.util.ChannelManager.ChannelType;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.core.handle.ClientChannelManagerHandler;
import com.anur.io.core.client.ReconnectableClient;
import com.anur.io.core.handle.StrMsgConsumeHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LineBasedFrameDecoder;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 *
 * 负责连接其他的服务器
 */
public class ElectClient extends ReconnectableClient {

    /**
     * 将如何消费消息的权利交给上级，将业务处理从Client中剥离
     */
    private BiConsumer<ChannelHandlerContext, String> msgConsumer;

    public ElectClient(String serverName, String host, int port, ShutDownHooker shutDownHooker,
        BiConsumer<ChannelHandlerContext, String> msgConsumer) {
        super(serverName, host, port, shutDownHooker);
        this.msgConsumer = msgConsumer;
    }

    @Override
    public ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline) {
        return channelPipeline.addFirst(new ClientChannelManagerHandler(ChannelType.ELECT, serverName))
                              .addLast(new LineBasedFrameDecoder(Integer.MAX_VALUE)) // 解决拆包粘包
                              .addLast(new StrMsgConsumeHandler(msgConsumer));// 业务处理逻辑处理器;// 将管道纳入统一管理
    }

    @Override
    public void howToRestart() {
        new ElectClient(this.serverName, this.host, this.port, this.shutDownHooker, this.msgConsumer).start();
    }
}
