package com.anur.io.elect.client;

import java.nio.charset.Charset;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.coder.Coder;
import com.anur.core.coder.Coder.DecodeWrapper;
import com.anur.core.coder.ProtocolEnum;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.elect.vote.model.VotesResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 *
 * 当要成为领导时，需要去连接其他的 ServerElectHandler，并去发送各种选举相关的消息
 */
public class ClientElectHandler extends ChannelInboundHandlerAdapter {

    private Logger logger = LoggerFactory.getLogger(ClientElectHandler.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Votes votes = new Votes();
        votes.setGeneration(2);
        votes.setServerName(InetSocketAddressConfigHelper.getServerName());

        ctx.writeAndFlush(Unpooled.copiedBuffer(Coder.encode(ProtocolEnum.CANVASSED, votes), Charset.defaultCharset()));

        votes.setServerName("hanabi.2");
        ctx.writeAndFlush(Unpooled.copiedBuffer(Coder.encode(ProtocolEnum.CANVASSED, votes), Charset.defaultCharset()));

        votes.setGeneration(1);
        ctx.writeAndFlush(Unpooled.copiedBuffer(Coder.encode(ProtocolEnum.CANVASSED, votes), Charset.defaultCharset()));

        votes.setGeneration(2);
        votes.setServerName("hanabi.2");
        ctx.writeAndFlush(Unpooled.copiedBuffer(Coder.encode(ProtocolEnum.CANVASSED, votes), Charset.defaultCharset()));
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        String str = ((ByteBuf) msg).toString(Charset.defaultCharset());
        logger.debug("数据读取：" + str);

        DecodeWrapper decodeWrapper = Coder.decode(str);

        VotesResponse votesResponse = (VotesResponse) decodeWrapper.object;
        logger.info(Optional.ofNullable(votesResponse)
                            .map(Votes::toString)
                            .orElse("没拿到正确的选票"));

        super.channelRead(ctx, msg);
    }
}