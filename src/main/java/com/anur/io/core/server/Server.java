package com.anur.io.core.server;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.core.handle.MsgConsumeHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;

/**
 * Created by Anur IjuoKaruKas on 2019/1/18
 *
 * 供其他端连接的套接字服务端入口
 */
public abstract class Server {

    protected final int port;

    protected ShutDownHooker shutDownHooker;

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
                           .localAddress(new InetSocketAddress(port))
                           .childHandler(new ChannelInitializer<SocketChannel>() {

                               @Override
                               protected void initChannel(SocketChannel socketChannel) {
                                   channelPipelineConsumer(socketChannel.pipeline());
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