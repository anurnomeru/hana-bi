package com.anur.io.elect.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 *
 * 负责连接其他的服务器
 */
public class ElectClient {

    private static List<EventLoopGroup> cc = new ArrayList<>();

    private final String serverName;

    private final String host;

    private final int port;

    private Logger logger = LoggerFactory.getLogger(ElectClient.class);

    private static ExecutorService RECONNECTOR = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ReConnector")
                                                                                                           .build());

    private CountDownLatch reconnect_latch;

    public static void main(String[] args) throws Exception {
        new ElectClient("hanabi_test", "localhost", 10000).start();
    }

    public ElectClient(String serverName, String host, int port) {
        this.reconnect_latch = new CountDownLatch(1);
        this.serverName = serverName;
        this.host = host;
        this.port = port;
    }

    public void start() {
        RECONNECTOR.submit(() -> {
            try {
                reconnect_latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            new ElectClient(serverName, host, port).start();
        });

        EventLoopGroup eventExecutors = new NioEventLoopGroup();
        cc.add(eventExecutors);

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventExecutors)
                     .channel(NioSocketChannel.class)
                     .handler(new ChannelInitializer<SocketChannel>() {

                         @Override
                         protected void initChannel(SocketChannel socketChannel) throws Exception {
                             socketChannel.pipeline()
                                          .addLast(
                                              // 这个 handler 用于开启心跳检测
                                              "IdleStateHandler", new IdleStateHandler(60, 60, 60, TimeUnit.SECONDS))
                                          .addLast("ClientHeartbeatHandler", new ClientHeartbeatHandler())
                                          .addLast("ClientElectHandler", new ClientElectHandler());
                         }
                     });

            ChannelFuture channelFuture = bootstrap.connect(host, port);
            channelFuture.addListener(future -> {
                if (!future.isSuccess()) {

                    if (((ChannelFuture) future).channel()
                                                .eventLoop()
                                                .isShuttingDown()) {
                        return;
                    }

                    logger.info("与节点 {} [{}:{}] 的连接异常，正在重连 ...", serverName, host, port);
                    ((ChannelFuture) future).channel()
                                            .close();

                    reconnect_latch.countDown();
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
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
    }
}
