package com.anur.core;

import java.util.Optional;
import java.util.function.BiConsumer;
import com.anur.core.coder.Coder;
import com.anur.core.coder.Coder.DecodeWrapper;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.elect.vote.model.VotesResponse;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.elect.client.ElectClient;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 *
 * 进行leader选举的核心控制类
 */
public class Elector implements Runnable {



    /**
     * 选举客户端，也就是发起投票者，要去连接其他节点的服务端，还没有进入选举阶段时，可以不连接。
     */
    private ElectClient electClient;

    /**
     * 关闭连接服务器socket的钩子
     */
    private ShutDownHooker clientShutDownHooker;

    /**
     * 初始化Elector
     */
    public void init() {

        BiConsumer<ChannelHandlerContext, String> clientMsgConsumer = (ctx, msg) -> {
            DecodeWrapper decodeWrapper = Coder.decode(msg);

            VotesResponse votesResponse = (VotesResponse) decodeWrapper.object;
            logger.info(Optional.ofNullable(votesResponse)
                                .map(Votes::toString)
                                .orElse("没拿到正确的选票"));
        };

        this.clientShutDownHooker = new ShutDownHooker(" ----------------- 终止连接其他选举节点！ ----------------- ");

        this.voter = new Voter();
        this.electClient = new ElectClient("hanabi_test", "localhost", 10000, clientMsgConsumer, this.clientShutDownHooker);
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
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                serverShutDownHooker.shutdown();
            });

            pool.submit(() -> {
                try {
                    Thread.sleep(32000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                clientShutDownHooker.shutdown();
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
