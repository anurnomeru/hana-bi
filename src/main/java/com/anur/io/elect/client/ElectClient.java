package com.anur.io.elect.client;

import java.net.InetSocketAddress;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 */
public class ElectClient {

    private final String host;

    private final int port;

    public static void main(String[] args) throws Exception {
        new ElectClient("localhost", 10000).start();
    }

    public ElectClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() throws Exception {
        EventLoopGroup eventExecutors = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventExecutors)
                     .channel(NioSocketChannel.class)
                     .remoteAddress(new InetSocketAddress(host, port))
                     .handler(new ChannelInitializer<SocketChannel>() {

                         @Override
                         protected void initChannel(SocketChannel socketChannel) throws Exception {
                             socketChannel.pipeline()
                                          .addLast(new ClientElectHandler());
                         }
                     });

            ChannelFuture channelFuture = bootstrap.connect()
                                                   .sync();
            channelFuture.channel()
                         .closeFuture()
                         .sync();
        } catch (InterruptedException e) {
            eventExecutors.shutdownGracefully()
                          .sync();
        }
    }
}
