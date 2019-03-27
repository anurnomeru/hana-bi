package com.anur.core.coordinate;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.util.HanabiExecutors;
import com.anur.core.util.ShutDownHooker;
import com.anur.io.coordinate.client.CoordinateClient;
import com.anur.io.core.coder.CoordinateEncoder;
import com.anur.io.store.common.Operation;
import com.anur.io.store.common.OperationTypeEnum;
import com.anur.io.store.operationset.ByteBufferOperationSet;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;

/**
 * Created by Anur IjuoKaruKas on 2/12/2019
 *
 * 集群内通讯、协调服务器操作类客户端，负责协调相关的业务
 */
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
    private static BiConsumer<ChannelHandlerContext, ByteBuffer> CLIENT_MSG_CONSUMER = (ctx, msg) -> {
        Operation operation = new Operation(msg);
    };

    /**
     * 需要在 channelPipeline 上挂载什么
     */
    private static Consumer<ChannelPipeline> PIPE_LINE_ADDER = c -> c.addFirst(new Register());

    /**
     * Coordinate 初始化连接时的注册器
     */
    static class Register extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            Operation operation = new Operation(OperationTypeEnum.REGISTER, InetSocketAddressConfigHelper.getServerName(), "");
            ByteBufferOperationSet byteBufferOperationSet = new ByteBufferOperationSet(operation.getByteBuffer());
            CoordinateEncoder.calcCrcAndFlushMsg(ctx.channel(), byteBufferOperationSet.getByteBuffer());
        }
    }

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
                        INSTANCE.shutDown();
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
        this.coordinateClient = new CoordinateClient(hanabiNode.getServerName(), hanabiNode.getHost(),
            hanabiNode.getCoordinatePort(), this.serverShutDownHooker, CLIENT_MSG_CONSUMER, PIPE_LINE_ADDER);
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

    /**
     * 关闭某个协调器的连接
     */
    public synchronized void shutDown() {
        this.serverShutDownHooker.shutdown();
    }

    /**
     * 关闭协调器连接
     */
    public static void shutDownInstance(String log) {
        Optional.ofNullable(INSTANCE)
                .ifPresent(c -> {
                    logger.info(log);
                    c.shutDown();
                });
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
