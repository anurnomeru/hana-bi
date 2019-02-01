package com.anur.io.elect.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.util.ShutDownHooker;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Created by Anur IjuoKaruKas on 2/1/2019
 */
public class ShutDownHandler extends ChannelInboundHandlerAdapter {

    private ShutDownHooker shutDownHooker;

    private Logger logger = LoggerFactory.getLogger(ShutDownHandler.class);

    public ShutDownHandler(ShutDownHooker shutDownHooker) {
        this.shutDownHooker = shutDownHooker;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        shutDownHooker.shutDownRegister(aVoid ->
            ctx.close()
        );
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("已经销毁====================");
        super.channelInactive(ctx);
    }
}
