package com.anur.core;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiCluster;
import com.anur.core.coder.Coder;
import com.anur.core.coder.Coder.DecodeWrapper;
import com.anur.core.coder.ProtocolEnum;
import com.anur.core.elect.vote.base.VoteController;
import com.anur.core.elect.vote.model.Canvass;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.elect.vote.model.VotesResponse;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.elect.client.ElectClient;
import com.anur.io.elect.server.ElectServer;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 *
 * 进行leader选举的核心控制类
 */
public class Elector implements Runnable {

    private Logger logger = LoggerFactory.getLogger(Elector.class);

    /**
     * 选举客户端，也就是发起投票者，要去连接其他节点的服务端，还没有进入选举阶段时，可以不连接。
     */
    private ElectClient electClient;

    /**
     * 选举服务端，需要常驻
     */
    private ElectServer electServer;

    /**
     * 投票控制器
     */
    private Voter voter;

    /**
     * 启动latch
     */
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 启动池
     */
    private ExecutorService pool = Executors.newFixedThreadPool(Integer.MAX_VALUE);

    /**
     * 关闭服务器的钩子
     */
    private ShutDownHooker serverShutDownHooker;

    /**
     * 关闭连接服务器socket的钩子
     */
    private ShutDownHooker clientShutDownHooker;

    public static void main(String[] args) {
        Elector elector = new Elector();
        elector.init();

        Thread thread = new Thread(elector);
        thread.start();
    }

    /**
     * 初始化Elector
     */
    public void init() {
        BiConsumer<ChannelHandlerContext, String> serverMsgConsumer = (ctx, msg) -> {
            DecodeWrapper decodeWrapper = Coder.decode(msg);
            switch (decodeWrapper.protocolEnum) {
            case CANVASSED:
                Votes votes = (Votes) decodeWrapper.object;
                Canvass canvass = voter.vote(votes);

                VotesResponse myVote;

                // 返回true代表同意某个节点来的投票
                if (canvass.isAgreed()) {
                    logger.info("来自节点 {}，世代 {}，的选票请求有效，返回选票", votes.getServerName(), votes.getGeneration());

                    // 那么则生成一张选票，返回给服务器
                    myVote = new VotesResponse(canvass.getGeneration(), InetSocketAddressConfigHelper.getServerName(), true);
                } else {
                    logger.info("来自节点 {}，世代 {}，的选票请求无效", votes.getServerName(), votes.getGeneration());

                    // 否则生成一张无效选票，返回给服务器
                    myVote = new VotesResponse(canvass.getGeneration(), InetSocketAddressConfigHelper.getServerName(), false);
                }

                ctx.writeAndFlush(Unpooled.copiedBuffer(Coder.encode(ProtocolEnum.CANVASSED_RESPONSE, myVote), Charset.defaultCharset()));
                break;
            default:
                break;
            }
        };

        BiConsumer<ChannelHandlerContext, String> clientMsgConsumer = (ctx, msg) -> {
            DecodeWrapper decodeWrapper = Coder.decode(msg);

            VotesResponse votesResponse = (VotesResponse) decodeWrapper.object;
            logger.info(Optional.ofNullable(votesResponse)
                                .map(Votes::toString)
                                .orElse("没拿到正确的选票"));
        };

        this.serverShutDownHooker = new ShutDownHooker();
        this.clientShutDownHooker = new ShutDownHooker();

        this.voter = new Voter();
        this.electServer = new ElectServer(InetSocketAddressConfigHelper.getServerPort(), serverMsgConsumer, serverShutDownHooker);
        this.electClient = new ElectClient("hanabi_test", "localhost", 10000, clientMsgConsumer);
        countDownLatch.countDown();
    }

    @Override
    public void run() {
        try {
            countDownLatch.await();
            pool.submit(() -> {
                electServer.start();
            });
            pool.submit(() -> {
                electClient.start();
            });

            pool.submit(() -> {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("准备关闭服务器！");
                serverShutDownHooker.shutdown();
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static class Voter extends VoteController {

        @Override
        protected void becomeLeader(List<HanabiCluster> hanabiClusterList) {

        }

        @Override
        protected void askForVote(List<HanabiCluster> hanabiClusterList) {

        }
    }
}
