package com.anur.core.coordinate;

import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.coder.Coder;
import com.anur.core.coder.Coder.DecodeWrapper;
import com.anur.core.util.HanabiExecutors;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.coordinate.client.CoordinateClient;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by Anur IjuoKaruKas on 2/12/2019
 *
 * 集群内通讯、协调服务器操作类客户端，负责协调相关的业务
 */
@SuppressWarnings("ALL")
public class CoordinateClientOperator implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(CoordinateClientOperator.class);

    /**
     * 关闭本服务的钩子
     */
    private ShutDownHooker serverShutDownHooker;

    /**
     * 启动latch
     */
    private CountDownLatch initialLatch = new CountDownLatch(2);

    /**
     * 协调客户端
     */
    private CoordinateClient coordinateClient;

    /**
     * 要连接的节点的信息
     */
    private HanabiNode hanabiNode;

    private static volatile CoordinateClientOperator INSTANCE;

    /**
     * 如何消费消息
     */
    private static BiConsumer<ChannelHandlerContext, String> CLIENT_MSG_CONSUMER = (ctx, msg) -> {
        DecodeWrapper decodeWrapper = Coder.decode(msg);
    };

    /**
     * 连接Leader节点的协调器连接，只能同时存在一个，如果要连接新的Leader，则需要将旧节点的连接关闭
     */
    public static CoordinateClientOperator getInstance(HanabiNode hanabiNode) {
        if (INSTANCE == null || !hanabiNode.getServerName()
                                           .equals(INSTANCE.hanabiNode)) {
            synchronized (CoordinateClientOperator.class) {

                if (INSTANCE == null || !hanabiNode.getServerName()
                                                   .equals(INSTANCE.hanabiNode)) {

                    if (INSTANCE != null && !hanabiNode.getServerName()
                                                       .equals(INSTANCE.hanabiNode)) {
                        INSTANCE.ShutDown();
                    }

                    INSTANCE = new CoordinateClientOperator(hanabiNode);
                    INSTANCE.init();
                    HanabiExecutors.submit(INSTANCE);
                }
            }
        }
        return INSTANCE;
    }

    public CoordinateClientOperator(HanabiNode hanabiNode) {
        this.hanabiNode = hanabiNode;
    }

    /**
     * 初始化Coordinator
     */
    private void init() {
        this.serverShutDownHooker = new ShutDownHooker(
            String.format("终止与协调节点 %s [%s:%s] 的连接", hanabiNode.getServerName(), hanabiNode.getHost(), hanabiNode.getCoordinatePort()));
        this.coordinateClient = new CoordinateClient(hanabiNode.getServerName(), hanabiNode.getHost(), hanabiNode.getCoordinatePort(), this.serverShutDownHooker, CLIENT_MSG_CONSUMER);
        initialLatch.countDown();
    }

    /**
     * 启动client，没事可以多调用，并不会启动多个连接
     */
    public void tryStartWhileDisconnected() {
        if (this.serverShutDownHooker.isShutDown()) {// 如果以前就创建过这个client，但是中途关掉了，直接重启即可
            logger.debug("正在重新建立与协调器节点 {} [{}:{}] 的连接", hanabiNode.getServerName(), hanabiNode.getHost(), hanabiNode.getCoordinatePort());
            this.serverShutDownHooker.reset();
            HanabiExecutors.submit(this);
        } else {
            initialLatch.countDown();// 如果没创建过，则直接将其启动
        }
    }

    public synchronized void ShutDown() {
        this.serverShutDownHooker.shutdown();
    }

    @Override
    public void run() {
        try {
            initialLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.debug("正在建立与协调器节点 {} [{}:{}] 的连接", hanabiNode.getServerName(), hanabiNode.getHost(), hanabiNode.getCoordinatePort());
        coordinateClient.start();
    }
}
