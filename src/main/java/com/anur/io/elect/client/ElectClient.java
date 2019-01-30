package com.anur.io.elect.client;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.coder.Coder;
import com.anur.core.coder.Coder.DecodeWrapper;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.elect.vote.model.VotesResponse;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 *
 * 负责连接其他的服务器
 */
public class ElectClient {

    private final String serverName;

    private final String host;

    private final int port;

    private Logger logger = LoggerFactory.getLogger(ElectClient.class);

    private CountDownLatch reconnectLatch;

    /**
     * 将如何消费消息的权利交给上级，将业务处理从Handler中隔离
     */
    private BiConsumer<ChannelHandlerContext, String> msgConsumer;

    private static ExecutorService RECONNECT_MANAGER = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ReConnector")
                                                                                                                 .build());

    public ElectClient(String serverName, String host, int port, BiConsumer<ChannelHandlerContext, String> msgConsumer) {
        this.reconnectLatch = new CountDownLatch(1);
        this.serverName = serverName;
        this.host = host;
        this.port = port;
        this.msgConsumer = msgConsumer;
    }

    public void start()  {
        RECONNECT_MANAGER.submit(() -> {
            try {
                reconnectLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            new ElectClient(serverName, host, port, msgConsumer).start();
        });

        EventLoopGroup eventExecutors = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventExecutors)
                     .channel(NioSocketChannel.class)
                     .handler(new ChannelInitializer<SocketChannel>() {

                         @Override
                         protected void initChannel(SocketChannel socketChannel) throws Exception {
                             socketChannel.pipeline()
                                          .addLast("LineBasedFrameDecoder", new LineBasedFrameDecoder(Integer.MAX_VALUE))
                                          .addLast(
                                              // 这个 handler 用于开启心跳检测
                                              "IdleStateHandler", new IdleStateHandler(10, 10, 10, TimeUnit.SECONDS))
                                          .addLast("ClientHeartbeatHandler", new ClientReconnectHandler(serverName, reconnectLatch))
                                          .addLast("ClientElectHandler", new ClientElectHandler(msgConsumer));
                         }
                     });

            ChannelFuture channelFuture = bootstrap.connect(host, port);
            channelFuture.addListener(future -> {
                if (!future.isSuccess()) {

                    if (reconnectLatch.getCount() == 1) {
                        logger.info("与节点 {} [{}] 的连接异常，正在重连 ...", serverName, host, ((ChannelFuture) future).channel()
                                                                                                            .remoteAddress());
                    }

                    reconnectLatch.countDown();
                }
            });

            channelFuture.channel()
                         .closeFuture()
                         .sync();
        } catch (InterruptedException ignore) {
        } finally {
            try {
                eventExecutors.shutdownGracefully()
                              .sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
