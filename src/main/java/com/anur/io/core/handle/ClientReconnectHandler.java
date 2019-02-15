package com.anur.io.core.handle;

import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Created by Anur IjuoKaruKas on 1/29/2019
 *
 * 客户端断线重连~~
 */
public class ClientReconnectHandler extends ChannelInboundHandlerAdapter {

    private String serverName;

    private CountDownLatch reconnectLatch;

    private Logger logger = LoggerFactory.getLogger(ClientReconnectHandler.class);

    public ClientReconnectHandler(String serverName, CountDownLatch reconnectLatch) {
        this.serverName = serverName;
        this.reconnectLatch = reconnectLatch;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        logger.debug("连接节点 {} [{}] 成功", serverName, ctx.channel()
                                                       .remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

        if (reconnectLatch.getCount() == 1) {
            logger.debug("与节点 {} [{}] 的连接断开，准备进行重连 ...", serverName, ctx.channel()
                                                                        .remoteAddress());
        }
        ctx.close();
        reconnectLatch.countDown();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.channelInactive(ctx);
        if (reconnectLatch.getCount() == 1) {
            logger.debug("与节点 {} [{}] 的连接断开，准备进行重连 ...", serverName, ctx.channel()
                                                                        .remoteAddress());
        }
        ctx.close();
        logger.debug("与节点 {} [{}] 的连接断开，原因：{}", serverName, ctx.channel()
                                                               .remoteAddress(), cause);
    }
}
