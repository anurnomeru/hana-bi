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
//
//    @Override
//    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
//        super.userEventTriggered(ctx, evt);
//
//        if (evt instanceof IdleStateEvent) {
//            IdleStateEvent event = (IdleStateEvent) evt;
//            if (event.state()
//                     .equals(IdleState.READER_IDLE)) {// 长期没收到服务器推送数据
//
//                if (reconnectLatch.getCount() == 1) {
//                    logger.debug("长时间没有收到节点 {} [{}] 的消息，准备进行重连 ...", serverName, ctx.channel()
//                                                                                   .remoteAddress());
//                }
//
//                ctx.close();
//                reconnectLatch.countDown();
//            } else if (event.state()
//                            .equals(IdleState.WRITER_IDLE)) {// 长期未向服务器发送数据
//
//            } else if (event.state()
//                            .equals(IdleState.ALL_IDLE)) {// 两者都
//            }
//        }
//    }

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
}
