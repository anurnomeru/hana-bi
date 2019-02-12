package com.anur.io.core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.util.ShutDownHooker;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 *
 * 负责连接其他的服务器
 */
public abstract class Client {

    protected final String serverName;

    protected final String host;

    protected final int port;

    protected Logger logger = LoggerFactory.getLogger(Client.class);

    protected CountDownLatch reconnectLatch;

    protected ShutDownHooker shutDownHooker;

    /**
     * 将如何消费消息的权利交给上级，将业务处理从Handler中隔离
     */
    protected BiConsumer<ChannelHandlerContext, String> msgConsumer;

    protected static ExecutorService RECONNECT_MANAGER = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ReConnector")
                                                                                                                   .build());

    public abstract ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline);

    public abstract void howToRestart();

    public Client(String serverName, String host, int port, BiConsumer<ChannelHandlerContext, String> msgConsumer, ShutDownHooker shutDownHooker) {
        this.reconnectLatch = new CountDownLatch(1);
        this.serverName = serverName;
        this.host = host;
        this.port = port;
        this.msgConsumer = msgConsumer;
        this.shutDownHooker = shutDownHooker;
    }

    public void start() {

        RECONNECT_MANAGER.submit(() -> {
            try {
                reconnectLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (shutDownHooker.isShutDown()) {
                logger.info("与节点 {} [{}:{}] 的连接已被终止，无需再进行重连", serverName, host, port);
            } else {
                logger.info("正在重新连接节点 {} [{}:{}] ...", serverName, host, port);
                this.howToRestart();
            }
        });

        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                     .channel(NioSocketChannel.class)
                     .handler(new ChannelInitializer<SocketChannel>() {

                         @Override
                         protected void initChannel(SocketChannel socketChannel) throws Exception {
                             channelPipelineConsumer(socketChannel.pipeline()
                                                                  .addLast("LineBasedFrameDecoder", new LineBasedFrameDecoder(Integer.MAX_VALUE)) // 解决拆包粘包
                                                                  .addLast("ClientReconnectHandler", new ClientReconnectHandler(serverName, reconnectLatch)) // 引入重连机制
                                                                  .addLast("ClientMsgConsumeHandler", new ClientMsgConsumeHandler(msgConsumer)));// 业务处理逻辑处理器
                         }
                     });

            ChannelFuture channelFuture = bootstrap.connect(host, port);
            channelFuture.addListener(future -> {
                if (!future.isSuccess()) {
                    if (reconnectLatch.getCount() == 1) {
                        logger.info("连接节点 {} [{}:{}] 失败，准备进行重连 ...", serverName, host, port);
                    }

                    reconnectLatch.countDown();
                }
            });

            shutDownHooker.shutDownRegister(aVoid -> group.shutdownGracefully());

            channelFuture.channel()
                         .closeFuture()
                         .sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                group.shutdownGracefully()
                     .sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
