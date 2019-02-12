package com.anur.core.elect;

import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.coder.Coder;
import com.anur.core.coder.Coder.DecodeWrapper;
import com.anur.core.coder.ProtocolEnum;
import com.anur.core.elect.model.HeartBeat;
import com.anur.core.elect.model.VotesResponse;
import com.anur.core.elect.model.Votes;
import com.anur.core.util.HanabiExecutors;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.elect.server.ElectServer;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by Anur IjuoKaruKas on 2/1/2019
 *
 * 选举服务器操作类服务端，负责选举相关的业务
 */
public class ElectServerOperator implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ElectServerOperator.class);

    private volatile static ElectServerOperator INSTANCE;

    /**
     * 关闭本服务的钩子
     */
    private ShutDownHooker serverShutDownHooker;

    /**
     * 启动latch
     */
    private CountDownLatch initialLatch = new CountDownLatch(2);

    /**
     * 选举服务端，需要常驻
     */
    private ElectServer electServer;

    /**
     * 如何去消费消息
     */
    private static BiConsumer<ChannelHandlerContext, String> SERVER_MSG_CONSUMER = (ctx, msg) -> {
        DecodeWrapper decodeWrapper = Coder.decode(msg);
        VotesResponse votesResponse;
        ElectOperator.getInstance()
                     .updateGenWhileReceiveHigherGen(decodeWrapper.getServerName(), decodeWrapper.getGeneration());
        switch (decodeWrapper.getProtocolEnum()) {
        case VOTES_REQUEST:
            Votes votes = (Votes) decodeWrapper.getObject();
            votesResponse = ElectOperator.getInstance()
                                         .receiveVotes(votes);

            // 返回true代表同意某个节点来的投票
            if (votesResponse.isAgreed()) {
                logger.debug("来自节点 {}，世代 {}，的选票请求有效，返回选票", votes.getServerName(), votes.getGeneration());
            } else {
                logger.debug("来自节点 {}，世代 {}，的选票请求无效", votes.getServerName(), votes.getGeneration());
            }

            ctx.writeAndFlush(Unpooled.copiedBuffer(Coder.encode(ProtocolEnum.VOTES_RESPONSE, votesResponse), Charset.defaultCharset()));
            break;

        case HEART_BEAT:
            HeartBeat heartBeat = (HeartBeat) decodeWrapper.getObject();
            ElectOperator.getInstance()
                         .receiveHeatBeat(heartBeat.getServerName(), decodeWrapper.getGeneration());
        default:
            break;
        }
    };

    /**
     * 选举服务端是个纯单例，没什么特别的
     */
    public static ElectServerOperator getInstance() {
        if (INSTANCE == null) {
            synchronized (ElectServerOperator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ElectServerOperator();
                    INSTANCE.init();
                    HanabiExecutors.submit(INSTANCE);
                }
            }
        }
        return INSTANCE;
    }

    /**
     * 初始化Elector
     */
    public void init() {
        this.serverShutDownHooker = new ShutDownHooker(String.format(" ----------------- 终止选举服务器的套接字接口 %s 的监听！ ----------------- ", InetSocketAddressConfigHelper.getServerElectionPort()));
        this.electServer = new ElectServer(InetSocketAddressConfigHelper.getServerElectionPort(), SERVER_MSG_CONSUMER, serverShutDownHooker);
        initialLatch.countDown();
    }

    public void start() {
        initialLatch.countDown();
    }

    /**
     * 优雅地关闭选举服务器
     */
    public void shutDown() {
        serverShutDownHooker.shutdown();
    }

    @Override
    public void run() {
        try {
            initialLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.debug("选举服务器正在启动...");
        electServer.start();
    }
}
