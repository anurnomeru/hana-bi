package com.anur.core;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by Anur IjuoKaruKas on 2/1/2019
 *
 * 选举客户端操作类，负责选举相关的业务
 */
public class ElectClientOperator {

    private static Logger logger = LoggerFactory.getLogger(ElectClientOperator.class);

    private volatile static ElectClientOperator INSTANCE;

    /**
     * 关闭本服务的钩子
     */
    private ShutDownHooker clientShutDownHooker;

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

        /** 1、移除上个世代的所有定时任务 */

        /** 2、建立与其他节点的连接，如果连接还未建立，则建立连接，如果已经建立，则不必再建立连接 */
        hanabiClusterList.forEach(hanabiCluster -> {
            //            if (!hanabiCluster.isLocalNode()) {
            ElectClient electClient = new ElectClient(hanabiCluster.getServerName(), hanabiCluster.getHost(), hanabiCluster.getElectionPort(),
                this.clientMsgConsumer, this.clientShutDownHooker);

            // 建立连接
            HanabiExecutors.submit(electClient::start);

            HanabiExecutors.submit(() -> {
                Channel channel;
                while ((channel = ChannelManager.getInstance(ChannelType.ELECT)
                                                .getChannel(hanabiCluster.getServerName())) == null) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                channel.writeAndFlush(Unpooled.copiedBuffer(Coder.encode(ProtocolEnum.CANVASSED, votes), Charset.defaultCharset()));
            });
            //            }
        });
    }
}
