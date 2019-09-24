package com.anur.io.core.client;

import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.util.HanabiExecutors;
import com.anur.util.ShutDownHooker;
import com.anur.io.core.handle.ClientReconnectHandler;
import com.anur.io.core.handle.ErrorHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 *
 * 可重连的客户端
 */
public abstract class ReconnectableClient {

    protected final String serverName;

    protected final String host;

    protected final int port;

    protected Logger logger = LoggerFactory.getLogger(ReconnectableClient.class);

    private CountDownLatch reconnectLatch;

    protected ShutDownHooker shutDownHooker;

    public abstract ChannelPipeline channelPipelineConsumer(ChannelPipeline channelPipeline);

    public abstract void howToRestart();

    public ReconnectableClient(String serverName, String host, int port, ShutDownHooker shutDownHooker) {
        this.reconnectLatch = new CountDownLatch(1);
        this.serverName = serverName;
        this.host = host;
        this.port = port;
        this.shutDownHooker = shutDownHooker;
    }

    public void start() {

        HanabiExecutors.INSTANCE.execute(() -> {
            try {
                reconnectLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (shutDownHooker.isShutDown()) {
                logger.debug("与节点 {} [{}:{}] 的连接已被终止，无需再进行重连", serverName, host, port);
            } else {
                logger.trace("正在重新连接节点 {} [{}:{}] ...", serverName, host, port);
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
                             channelPipelineConsumer(
                                 socketChannel.pipeline()
                                              .addLast(new ClientReconnectHandler(serverName, reconnectLatch))).addLast(new ErrorHandler()); // 引入重连机制
                         }
                     });

            ChannelFuture channelFuture = bootstrap.connect(host, port);
            channelFuture.addListener(future -> {
                if (!future.isSuccess()) {
                    if (reconnectLatch.getCount() == 1) {
                        logger.trace("连接节点 {} [{}:{}] 失败，准备进行重连 ...", serverName, host, port);
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
