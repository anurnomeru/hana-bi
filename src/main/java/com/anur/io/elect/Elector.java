package com.anur.io.elect;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
import com.anur.io.elect.client.ElectClient;
import com.anur.io.elect.server.ElectServer;
import io.netty.buffer.Unpooled;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 *
 * 进行leader选举的核心控制类
 */
public class Elector implements Runnable {

    private Logger logger = LoggerFactory.getLogger(Elector.class);

    private ElectClient electClient;

    private ElectServer electServer;

    private Voter voter;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

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
        this.voter = new Voter();
        this.electServer =
            new ElectServer(InetSocketAddressConfigHelper.getServerPort(), (ctx, msg) -> {
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
            });
        countDownLatch.countDown();
    }

    @Override
    public void run() {
        try {
            countDownLatch.await();
            electServer.start();
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
