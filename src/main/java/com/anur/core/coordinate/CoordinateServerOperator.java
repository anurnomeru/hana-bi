package com.anur.core.coordinate;

import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.io.core.coder.ElectCoder;
import com.anur.io.core.coder.ElectCoder.ElectDecodeWrapper;
import com.anur.core.util.HanabiExecutors;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.coordinate.server.CoordinateServer;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by Anur IjuoKaruKas on 2/12/2019
 *
 * 集群内通讯、协调服务器操作类服务端，负责协调相关的业务
 */
public class CoordinateServerOperator implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(CoordinateServerOperator.class);

    private volatile static CoordinateServerOperator INSTANCE;

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
    private CoordinateServer coordinateServer;

    /**
     * 如何去消费消息
     */
    private static BiConsumer<ChannelHandlerContext, String> SERVER_MSG_CONSUMER = (ctx, msg) -> {
        ElectDecodeWrapper decodeWrapper = ElectCoder.decode(msg);
    };

    /**
     * 协调器的服务端是个纯单例，没什么特别的
     */
    public static CoordinateServerOperator getInstance() {
        if (INSTANCE == null) {
            synchronized (CoordinateServerOperator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new CoordinateServerOperator();
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
        this.serverShutDownHooker = new ShutDownHooker(String.format("终止协调服务器的套接字接口 %s 的监听！", InetSocketAddressConfigHelper.getServerCoordinatePort()));
        this.coordinateServer = new CoordinateServer(InetSocketAddressConfigHelper.getServerCoordinatePort(), serverShutDownHooker, SERVER_MSG_CONSUMER);
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
        logger.info("协调服务器正在启动...");
        coordinateServer.start();
    }
}
