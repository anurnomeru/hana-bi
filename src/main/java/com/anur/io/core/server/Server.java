package com.anur.io.core.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.util.ShutDownHooker;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Created by Anur IjuoKaruKas on 2019/1/18
 *
 * 供其他端连接的套接字服务端入口
 */
public abstract class Server {

    private static Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final int port;

    private ShutDownHooker shutDownHooker;

    public Server(int port, ShutDownHooker shutDownHooker) {
        this.port = port;
        this.shutDownHooker = shutDownHooker;
    }

    public abstract ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline);

    public void start() {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(group)
                           .channel(NioServerSocketChannel.class)
                           .childHandler(new ChannelInitializer<SocketChannel>() {

                               @Override
                               protected void initChannel(SocketChannel socketChannel) {
                                   channelPipelineConsumer(socketChannel.pipeline());
                               }
                           })
                           // 保持连接
                           .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = serverBootstrap.bind(port);

            f.addListener(future -> {
                if (!future.isSuccess()) {
                    LOGGER.error("监听端口 {} 失败！项目启动失败！", port);
                    System.exit(1);
                }
            });

            shutDownHooker.shutDownRegister(aVoid -> group.shutdownGracefully());

            f.channel()
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
