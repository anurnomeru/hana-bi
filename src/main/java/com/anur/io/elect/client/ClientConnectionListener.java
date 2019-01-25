package com.anur.io.elect.client;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * Created by Anur IjuoKaruKas on 1/25/2019
 */
//public class ClientConnectionListener implements ChannelFutureListener {
//
//    private ImConnection imConnection = new ImConnection();
//
//    @Override
//    public void operationComplete(ChannelFuture channelFuture) throws Exception {
//        if (!channelFuture.isSuccess()) {
//            final EventLoop loop = channelFuture.channel()
//                                                .eventLoop();
//            loop.schedule(new Runnable() {
//
//                @Override
//                public void run() {
//                    System.err.println("服务端链接不上，开始重连操作...");
//                    imConnection.connect(ImClientApp.HOST, ImClientApp.PORT);
//                }
//            }, 1L, TimeUnit.SECONDS);
//        } else {
//            System.err.println("服务端链接成功...");
//        }
//    }
//}
