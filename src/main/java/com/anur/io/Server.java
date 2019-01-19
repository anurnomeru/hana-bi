package com.anur.io;

import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.io.handler.InitialHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
/**
 * Created by Anur IjuoKaruKas on 2019/1/18
 *
 * 供其他端连接的套接字服务端入口
 */
public class Server {

    private final int port;

    public Server(int port) {
        this.port = port;
    }

    static Logger logger = LoggerFactory.getLogger("123123");

    public static void main(String[] args) throws InterruptedException {
        logger.info("123123123");
        new Server(9876).start();
    }

    public void start() throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(group)
                           .channel(NioServerSocketChannel.class)
                           .localAddress(new InetSocketAddress(port))
                           .childHandler(new ChannelInitializer<SocketChannel>() {

                               @Override
                               protected void initChannel(SocketChannel socketChannel) {
                                   socketChannel.pipeline()
                                                .addLast(new InitialHandler());
                               }
                           });

            ChannelFuture f = serverBootstrap.bind()
                                             .sync();
            f.channel()
             .closeFuture()
             .sync();
        } finally {
            group.shutdownGracefully()
                 .sync();
        }
    }
}
