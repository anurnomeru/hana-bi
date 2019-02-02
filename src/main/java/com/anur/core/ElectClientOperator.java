package com.anur.core;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiCluster;
import com.anur.core.coder.Coder;
import com.anur.core.coder.Coder.DecodeWrapper;
import com.anur.core.coder.ProtocolEnum;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.elect.vote.model.VotesResponse;
import com.anur.core.util.ChannelManager;
import com.anur.core.util.ChannelManager.ChannelType;
import com.anur.core.util.HanabiExecutors;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.elect.client.ElectClient;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by Anur IjuoKaruKas on 2/1/2019
 *
 * 选举客户端操作类
 */
public class ElectClientOperator {

    private static Logger logger = LoggerFactory.getLogger(ElectClientOperator.class);

    private volatile static ElectClientOperator INSTANCE;

    /**
     * 关闭本服务的钩子
     */
    private ShutDownHooker clientShutDownHooker;

    /**
     * 启动latch
     */
    private CountDownLatch initialLatch = new CountDownLatch(2);

    /**
     * 是否正处于选举之中
     */
    private Semaphore electState;

    /**
     * 所有节点的连接
     */
    private Map<String, ElectClient> clusterMap;

    /**
     * 如何消费消息
     */
    private BiConsumer<ChannelHandlerContext, String> clientMsgConsumer = (ctx, msg) -> {
        DecodeWrapper decodeWrapper = Coder.decode(msg);

        VotesResponse votesResponse = (VotesResponse) decodeWrapper.object;
        logger.info(Optional.ofNullable(votesResponse)
                            .map(Votes::toString)
                            .orElse("没拿到正确的选票"));
    };

    public static ElectClientOperator getInstance() {
        if (INSTANCE == null) {
            synchronized (ElectServerOperator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ElectClientOperator();
                }
            }
        }

        return INSTANCE;
    }

    /**
     * 开始一场选举
     */
    public void beginSelection(List<HanabiCluster> hanabiClusterList, Votes votes) {
        this.clientShutDownHooker = new ShutDownHooker(" ----------------- 终止连接其他选举节点！ ----------------- ");

        hanabiClusterList.forEach(hanabiCluster -> {
            //            if (!hanabiCluster.isLocalNode()) {
            ElectClient electClient = new ElectClient(hanabiCluster.getServerName(), hanabiCluster.getHost(), hanabiCluster.getElectionPort(),
                this.clientMsgConsumer, this.clientShutDownHooker);

            // 启动服务
            HanabiExecutors.submit(() -> {
                Channel channel;
                // 开始投票
                while ((channel = ChannelManager.getInstance(ChannelType.ELECT)
                                                .getChannel(hanabiCluster.getServerName())) == null) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                channel.writeAndFlush(Coder.encode(ProtocolEnum.CANVASSED, votes));
                channel.writeAndFlush(Coder.encode(ProtocolEnum.CANVASSED, votes));
                channel.writeAndFlush(Coder.encode(ProtocolEnum.CANVASSED, votes));
                channel.writeAndFlush(Coder.encode(ProtocolEnum.CANVASSED, votes));
            });

            // 建立连接
            HanabiExecutors.submit(electClient::start);

            //            }
        });
    }
}
