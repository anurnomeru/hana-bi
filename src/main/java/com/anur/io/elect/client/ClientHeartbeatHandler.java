package com.anur.io.elect.client;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * Created by Anur IjuoKaruKas on 1/29/2019
 *
 * 与客户端的心跳机制，断线重连~~
 */
public class ClientHeartbeatHandler extends ChannelInboundHandlerAdapter {

    private String serverName;

    private CountDownLatch reconnectLatch;

    private Logger logger = LoggerFactory.getLogger(ClientHeartbeatHandler.class);

    private static Set<ChannelHandlerContext> t = new HashSet<>();

    public ClientHeartbeatHandler(String serverName, CountDownLatch reconnectLatch) {
        this.serverName = serverName;
        this.reconnectLatch = reconnectLatch;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        t.add(ctx);
        super.userEventTriggered(ctx, evt);
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state()
                     .equals(IdleState.READER_IDLE)) {// 长期没收到服务器推送数据

                if (reconnectLatch.getCount() == 0) {
                    logger.info("长时间没有收到节点 {} [{}] 的消息，正在重连 ...", serverName, ctx.channel()
                                                                                 .remoteAddress());
                }

                ctx.channel()
                   .close();
                reconnectLatch.countDown();
            } else if (event.state()
                            .equals(IdleState.WRITER_IDLE)) {// 长期未向服务器发送数据

            } else if (event.state()
                            .equals(IdleState.ALL_IDLE)) {// 两者都
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

        if (reconnectLatch.getCount() == 0) {
            logger.info("与节点 {} [{}] 的连接断开，正在重连 ...", serverName, ctx.channel()
                                                                     .remoteAddress());
        }

        ctx.channel()
           .close();
        reconnectLatch.countDown();
    }
}
