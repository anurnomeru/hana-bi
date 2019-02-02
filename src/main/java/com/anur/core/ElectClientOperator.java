package com.anur.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.coder.Coder;
import com.anur.core.coder.Coder.DecodeWrapper;
import com.anur.core.elect.vote.model.Votes;
import com.anur.core.elect.vote.model.VotesResponse;
import com.anur.core.util.HanabiExecutors;
import com.anur.core.util.ShutDownHooker;
import com.anur.exception.HanabiException;
import com.anur.io.elect.client.ElectClient;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by Anur IjuoKaruKas on 2/2/2019
 *
 * 选举服务器操作类客户端，负责选举相关的业务
 */
public class ElectClientOperator implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ElectServerOperator.class);

    private volatile static Map<String, ElectClientOperator> SERVER_INSTANCE_MAPPER = new HashMap<>();

    /**
     * 关闭本服务的钩子
     */
    private ShutDownHooker serverShutDownHooker;

    /**
     * 启动latch
     */
    private CountDownLatch initialLatch = new CountDownLatch(2);

    /**
     * 选举客户端
     */
    private ElectClient electClient;

    /**
     * 要连接服务
     */
    private String serverName;

    /**
     * 要连接的节点的信息
     */
    private HanabiNode hanabiNode;

    /**
     * 如何消费消息
     */
    private static BiConsumer<ChannelHandlerContext, String> CLIENT_MSG_CONSUMER = (ctx, msg) -> {
        DecodeWrapper decodeWrapper = Coder.decode(msg);

        VotesResponse votesResponse = (VotesResponse) decodeWrapper.object;
        logger.info(Optional.ofNullable(votesResponse)
                            .map(Votes::toString)
                            .orElse("没拿到正确的选票"));
    };

    public static ElectClientOperator getInstance(String serverName) {
        ElectClientOperator electClientOperator = SERVER_INSTANCE_MAPPER.get(serverName);

        if (electClientOperator == null) {
            synchronized (ElectClientOperator.class) {
                electClientOperator = SERVER_INSTANCE_MAPPER.get(serverName);
                if (electClientOperator == null) {
                    electClientOperator = new ElectClientOperator(serverName);
                    electClientOperator.init();
                    HanabiExecutors.submit(electClientOperator);
                }
            }
        }
        return SERVER_INSTANCE_MAPPER.get(serverName);
    }

    public ElectClientOperator(String serverName) {
        this.serverName = serverName;
    }

    /**
     * 初始化Elector
     */
    private void init() {
        this.hanabiNode = InetSocketAddressConfigHelper.getNode(serverName);

        if (hanabiNode.equals(HanabiNode.NOT_EXIST)) {
            throw new HanabiException("节点初始化失败，无法从集群配置中找到这个节点的信息。");
        }

        this.serverShutDownHooker = new ShutDownHooker(String.format(" ----------------- 终止选举服务器的套接字接口 %s 的监听！ ----------------- ", InetSocketAddressConfigHelper.getServerPort()));
        this.electClient = new ElectClient(this.serverName, hanabiNode.getHost(), hanabiNode.getElectionPort(), CLIENT_MSG_CONSUMER, this.serverShutDownHooker);
        initialLatch.countDown();
    }

    public void start() {
        initialLatch.countDown();
    }

    public void ShutDown() {
        this.serverShutDownHooker.shutdown();
    }

    /**
     * 重新连接
     */
    public void restart() {

    }

    @Override
    public void run() {
        try {
            initialLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("正在建立与节点 {} [{}] 的连接...", serverName, hanabiNode.getHost(), hanabiNode.getElectionPort());
        electClient.start();
    }
}
