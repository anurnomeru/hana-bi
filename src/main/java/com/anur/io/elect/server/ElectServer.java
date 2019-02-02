package com.anur.io.elect.server;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;
import com.anur.core.util.ShutDownHooker;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;

/**
 * Created by Anur IjuoKaruKas on 2019/1/18
 *
 * 供其他端连接的套接字服务端入口，ElectServer与选举逻辑无关，它只控制socket服务的开启和关闭
 */
public class ElectServer {

    private final int port;

    /**
     * 将如何消费消息的权利交给上级，将业务处理从Server中剥离
     */
    private BiConsumer<ChannelHandlerContext, String> msgConsumer;

    private ShutDownHooker shutDownHooker;

    public ElectServer(int port, BiConsumer<ChannelHandlerContext, String> msgConsumer, ShutDownHooker shutDownHooker) {
        this.port = port;
        this.msgConsumer = msgConsumer;
        this.shutDownHooker = shutDownHooker;
    }

    public void start() {
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
                                                .addLast(new LineBasedFrameDecoder(Integer.MAX_VALUE))
                                                .addLast(new ServerElectHandler(msgConsumer));
                               }
                           });

            ChannelFuture f = serverBootstrap.bind()
                                             .sync();

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
