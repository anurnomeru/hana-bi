package com.anur.io.elect.server;

import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.anur.core.elect.vote.model.Votes;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Created by Anur IjuoKaruKas on 2019/1/18
 *
 * 服务器的选举信息接收处理器，这个需要一直启动着，等待其他节点发送选票过来，
 */
public class ServerElectHandler extends SimpleChannelInboundHandler {

    private Logger logger = LoggerFactory.getLogger(ServerElectHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
        logger.info(o.toString());
        String str = ((ByteBuf) o).toString(Charset.defaultCharset());
        logger.info(str);

        Votes votes = JSON.parseObject(str, Votes.class);
        logger.info(votes.toString());
    }
}
