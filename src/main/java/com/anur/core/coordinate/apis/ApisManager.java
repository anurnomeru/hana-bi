package com.anur.core.coordinate.apis;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.CoordinateConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.coordinate.sender.CoordinateSender;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.struct.base.AbstractTimedStruct;
import com.anur.core.struct.coordinate.CommitResponse;
import com.anur.core.struct.coordinate.Commiter;
import com.anur.core.struct.coordinate.FetchResponse;
import com.anur.core.struct.coordinate.Fetcher;
import com.anur.core.struct.coordinate.Register;
import com.anur.core.struct.base.AbstractStruct;
import com.anur.core.coordinate.model.RequestProcessor;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.lock.ReentrantReadWriteLocker;
import com.anur.core.struct.coordinate.RegisterResponse;
import com.anur.core.util.ChannelManager;
import com.anur.core.util.ChannelManager.ChannelType;
import com.anur.exception.HanabiException;
import com.anur.io.store.common.FetchDataInfo;
import com.anur.io.store.log.LogManager;
import com.anur.io.store.prelog.ByteBufPreLogManager;
import com.anur.timewheel.TimedTask;
import com.anur.timewheel.Timer;
import io.netty.channel.Channel;
import io.netty.util.internal.StringUtil;

/**
 * Created by Anur IjuoKaruKas on 2019/3/27
 *
 * 此管理器负责大部分协调器的业务处理逻辑、并负责消息的重发、且保证这种消息类型，在收到回复之前，无法继续发同一种类型的消息
 *
 * 1、消息在没有收到回复之前，会定时重发。
 * 2、那么如何保证数据不被重复消费：我们以时间戳作为 key 的一部分，应答方需要在消费消息后，需要记录此时间戳，并不再消费比此时间戳小的消息。
 */
public class ApisManager extends ReentrantReadWriteLocker {

    private static volatile ApisManager INSTANCE;

    private static Map<OperationTypeEnum, OperationTypeEnum> ResponseAndRequestType = new HashMap<>();

    private Map<String, Map<OperationTypeEnum, Long>> requestLog = new HashMap<>();

    static {
        ResponseAndRequestType.put(OperationTypeEnum.REGISTER_RESPONSE, OperationTypeEnum.REGISTER);
        ResponseAndRequestType.put(OperationTypeEnum.FETCH_RESPONSE, OperationTypeEnum.FETCH);
        ResponseAndRequestType.put(OperationTypeEnum.COMMIT_RESPONSE, OperationTypeEnum.COMMIT);
    }

    public static ApisManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (ApisManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ApisManager();
                }
            }
        }
        return INSTANCE;
    }

    private final Logger logger = LoggerFactory.getLogger(ApisManager.class);

    /**
     * 此 map 确保对一个服务发送某个消息，在收到回复之前，不可以再次对其发送消息。（有自动重发机制）
     */
    private volatile Map<String, Map<OperationTypeEnum, RequestProcessor>> inFlight = new HashMap<>();

    /**
     * 重启此类，用于在重新选举后，刷新所有任务，不再执着于上个世代的任务
     */
    public void reboot() {
        this.writeLockSupplier(() -> {
            for (Entry<String, Map<OperationTypeEnum, RequestProcessor>> mmE : inFlight.entrySet()) {
                for (Entry<OperationTypeEnum, RequestProcessor> entry : mmE.getValue()
                                                                           .entrySet()) {
                    entry.getValue()
                         .cancel();
                }
            }
            inFlight = new HashMap<>();
            return null;
        });
    }

    /**
     * 接收到消息如何处理
     */
    public void receive(ByteBuffer msg, OperationTypeEnum typeEnum, Channel channel) {
        long requestTimestamp = msg.getLong(AbstractTimedStruct.TimestampOffset);

        String serverName = ChannelManager.getInstance(ChannelType.COORDINATE)
                                          .getChannelName(channel);

        if (writeLockSupplier(() -> {
            AtomicBoolean isAnewRequest = new AtomicBoolean(false);
            requestLog.compute(serverName, (s, m) -> {
                if (m == null) {
                    m = new HashMap<>();
                }
                m.compute(typeEnum, (operationTypeEnum, before) -> {
                        isAnewRequest.set(before == null || (requestTimestamp > before));
                        return isAnewRequest.get() ? requestTimestamp : before;
                    }
                );
                return m;
            });
            return isAnewRequest.get();
        })) {
            try {
                doReceive(serverName, msg, typeEnum, channel);
            } catch (Exception e) {
                logger.warn("在处理来自节点 {} 的 {} 请求时出现异常，可能原因 {}", serverName, typeEnum, e.getMessage());
                writeLockSupplier(() -> {
                    requestLog.compute(serverName, (s, m) -> {
                        if (m == null) {
                            m = new HashMap<>();
                        }
                        m.compute(typeEnum, (operationTypeEnum, before) -> null
                        );
                        return m;
                    });
                    return null;
                });
                e.printStackTrace();
            }
        }
    }

    private void doReceive(String serverName, ByteBuffer msg, OperationTypeEnum typeEnum, Channel channel) {
        switch (typeEnum) {
        case REGISTER:
            handleRegisterRequest(msg, channel);
            break;
        case FETCH:
            handleFetchRequest(msg, channel);
            break;
        case COMMIT:
            handleCommitRequest(msg, channel);
            break;
        default:
            logger.debug("receive responseness request");

            // todo 需要加入重试机制，在失败时回复发送方，否则发送方会一直等待
            // todo 需要加入防重复请求机制
            /*
             *  response 处理
             */
            OperationTypeEnum requestType = ResponseAndRequestType.get(typeEnum);
            if (StringUtil.isNullOrEmpty(serverName)) {
                throw new HanabiException("收到了来自已断开连接节点 " + serverName + " 关于 " + requestType.name() + " 的无效 response");
            }

            RequestProcessor rp = this.writeLockSupplier(() -> {
                RequestProcessor requestProcessor = Optional.ofNullable(inFlight.get(serverName))
                                                            .map(m -> m.get(requestType))
                                                            .orElse(null);
                if (requestProcessor == null || requestProcessor.isComplete()) {
                    throw new HanabiException("收到了来自节点 " + serverName + " 关于 " + requestType.name() + " 的无效 response");
                }

                if (typeEnum.equals(OperationTypeEnum.REGISTER_RESPONSE)) {

                }
                inFlight.get(serverName)
                        .remove(requestType);
                logger.debug("收到来自节点 {} 关于 {} 的 response", serverName, requestType.name());
                return requestProcessor;
            });
            rp.complete(msg);
            rp.afterCompleteReceive(typeEnum.name());
        }
    }

    /**
     * 协调子节点向父节点注册自己
     */
    private void handleRegisterRequest(ByteBuffer msg, Channel channel) {
        Register register = new Register(msg);
        logger.info("协调节点 {} 已注册到本节点", register.getServerName());
        ChannelManager.getInstance(ChannelType.COORDINATE)
                      .register(register.getServerName(), channel);
        send(register.getServerName(), new RegisterResponse(InetSocketAddressConfigHelper.getServerName()), RequestProcessor.REQUIRE_NESS);
    }

    /**
     * 协调子节点向父节点请求 Fetch 消息
     */
    private void handleFetchRequest(ByteBuffer msg, Channel channel) {
        Fetcher fetcher = new Fetcher(msg);
        String serverName = ChannelManager.getInstance(ChannelType.COORDINATE)
                                          .getChannelName(channel);

        logger.debug("收到来自协调节点 {} 的 Fetch 请求 {} ", serverName, fetcher.getFetchGAO());

        GenerationAndOffset canCommit = CoordinateApisManager.getINSTANCE()
                                                             .fetchReport(serverName, fetcher.getFetchGAO());

        send(serverName, new Commiter(canCommit), new RequestProcessor(
            byteBuffer -> {
                CommitResponse commitResponse = new CommitResponse(byteBuffer);
                CoordinateApisManager.getINSTANCE()
                                     .commitReport(serverName, commitResponse.getCommitGAO());
            }, null));

        // 为什么要。next，因为 fetch 过来的是客户端最新的 GAO 进度，而获取的要从 GAO + 1开始
        FetchDataInfo fetchDataInfo = LogManager.getINSTANCE()
                                                .getAfter(fetcher.getFetchGAO()
                                                                 .next());

        send(serverName, new FetchResponse(fetchDataInfo), RequestProcessor.REQUIRE_NESS);
    }

    /**
     * 子节点处理 commit 请求
     */
    private void handleCommitRequest(ByteBuffer msg, Channel channel) {
        Commiter commiter = new Commiter(msg);
        String serverName = ChannelManager.getInstance(ChannelType.COORDINATE)
                                          .getChannelName(channel);

        logger.debug("收到来自协调 Leader {} 的 commit 请求 {} ", serverName, commiter.getCanCommitGAO());

        ByteBufPreLogManager.getINSTANCE()
                            .commit(commiter.getCanCommitGAO());

        GenerationAndOffset commitGAO = ByteBufPreLogManager.getINSTANCE()
                                                            .getCommitGAO();
        send(serverName, new CommitResponse(commitGAO), RequestProcessor.REQUIRE_NESS);
    }

    /**
     * 此发送器保证【一个类型的消息】只能在收到回复前发送一次，类似于仅有 1 容量的Queue
     */
    public boolean send(String serverName, AbstractStruct command, RequestProcessor requestProcessor) {
        OperationTypeEnum typeEnum = command.getOperationTypeEnum();

        // 第一次不锁检查
        if (Optional.ofNullable(inFlight.get(serverName))
                    .map(enums -> enums.containsKey(typeEnum))
                    .orElse(false)) {
            logger.error("不应该出现这种情况，同时创建发送任务！" + typeEnum);
            return false;
        }

        return this.writeLockSupplier(() -> {

            // 双重锁检查
            if (Optional.ofNullable(inFlight.get(serverName))
                        .map(enums -> enums.containsKey(typeEnum))
                        .orElse(false)) {

                logger.debug("尝试创建发送到节点 {} 的 {} 任务失败，上次的指令还未收到 response", serverName, typeEnum.name());
                return false;
            } else {
                logger.debug("正在创建向 {} 发送 {} 的任务", serverName, typeEnum.name());
                inFlight.compute(serverName, (s, enums) -> {
                    if (enums == null) {
                        enums = new HashMap<>();
                    }
                    enums.put(typeEnum, requestProcessor);
                    sendImpl(serverName, command, requestProcessor, typeEnum);
                    return enums;
                });

                return true;
            }
        });
    }

    /**
     * 真正发送消息的方法，内置了重发机制
     *
     * TODO 先不进行重发，讲道理消息应该不会丢失
     */
    private void sendImpl(String serverName, AbstractStruct command, RequestProcessor requestProcessor, OperationTypeEnum operationTypeEnum) {
        this.readLockSupplier(() -> {

            if (requestProcessor == null || !requestProcessor.isComplete()) {

                CoordinateSender.send(serverName, command);

                if (requestProcessor == null) { // 是不需要回复的类型

                    writeLockSupplier(() -> inFlight.compute(serverName, (s, enums) -> {
                            if (enums == null) {
                                enums = new HashMap<>();
                            }
                            enums.remove(operationTypeEnum);
                            return enums;
                        })
                    );
                } else {
                    TimedTask task = new TimedTask(CoordinateConfigHelper.getFetchBackOfMs() * 3, () -> sendImpl(serverName, command, requestProcessor, operationTypeEnum));

                    if (Optional.ofNullable(inFlight.get(serverName))
                                .map(m -> m.get(operationTypeEnum))
                                .map(rp -> rp.registerTask(task))
                                .orElse(false)) {

                        logger.debug("正在重发向 {} 发送 {} 的任务", serverName, operationTypeEnum);
                        Timer.getInstance()// 扔进时间轮不断重试，直到收到此消息的回复
                             .addTask(task);
                    }
                }
            }
            return null;
        });
    }
}
