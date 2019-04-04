package com.anur.core.coordinate.apis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfigHelper;
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
public class InFlightApisManager extends ReentrantReadWriteLocker {

    private static volatile InFlightApisManager INSTANCE;

    private static Map<OperationTypeEnum, OperationTypeEnum> RequestAndResponseType = new HashMap<>();

    private static Map<OperationTypeEnum, OperationTypeEnum> ResponseAndRequestType = new HashMap<>();

    static {
        RequestAndResponseType.put(OperationTypeEnum.REGISTER, OperationTypeEnum.REGISTER_RESPONSE);
        RequestAndResponseType.put(OperationTypeEnum.FETCH, OperationTypeEnum.FETCH_RESPONSE);

        RequestAndResponseType.forEach((ek, ev) -> ResponseAndRequestType.put(ev, ek));
    }

    public static InFlightApisManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (InFlightApisManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new InFlightApisManager();
                }
            }
        }
        return INSTANCE;
    }

    private final Logger logger = LoggerFactory.getLogger(InFlightApisManager.class);

    /**
     * 此 map 确保对一个服务发送某个消息，在收到回复之前，不可以再次对其发送消息。（有自动重发机制）
     */
    private volatile Map<String, Map<OperationTypeEnum, RequestProcessor>> inFlight = new HashMap<>();

    /**
     * 重启此类，用于在重新选举后，刷新所有任务，不再执着于上个世代的任务
     */
    public void reboot() {
        this.writeLockSupplier(() -> {
            inFlight.forEach((s, m) -> m.forEach((o, t) -> t.cancel()));
            inFlight = new HashMap<>();
            return null;
        });
    }

    /**
     * 接收到消息如何处理
     */
    public void receive(ByteBuffer msg, OperationTypeEnum typeEnum, Channel channel) {
        if (RequestAndResponseType.containsKey(typeEnum)) {
            switch (typeEnum) {
            case REGISTER:
                handleRegisterRequest(msg, channel);
                break;
            case FETCH:
                handleFetchRequest(msg, channel);
                break;
            default:
                throw new HanabiException("不可能存在这种情况!!!");
            }
        } else {
            /*
             *  response 处理
             */
            OperationTypeEnum requestType = ResponseAndRequestType.get(typeEnum);
            String serverName = ChannelManager.getInstance(ChannelType.COORDINATE)
                                              .getChannelName(channel);
            if (StringUtil.isNullOrEmpty(serverName)) {
                throw new HanabiException("收到了来自已断开连接节点 " + serverName + " 关于 " + requestType.name() + " 的无效 response");
            }

            this.writeLockSupplier(() -> {
                RequestProcessor requestProcessor = Optional.ofNullable(inFlight.get(serverName))
                                                            .map(m -> m.get(requestType))
                                                            .orElse(null);
                if (requestProcessor == null || requestProcessor.isComplete()) {
                    throw new HanabiException("收到了来自节点 " + serverName + " 关于 " + requestType.name() + " 的无效 response");
                }

                requestProcessor.complete(msg);
                inFlight.get(serverName)
                        .remove(requestType);
                logger.info("收到来自节点 {} 关于 {} 的 response", serverName, requestType.name());
                return null;
            });
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
        send(register.getServerName(), new RegisterResponse(InetSocketAddressConfigHelper.getServerName()), RequestProcessor.REQUIRE_NESS());
    }

    /**
     * 协调子节点向父节点请求 Fetch 消息
     */
    private void handleFetchRequest(ByteBuffer msg, Channel channel) {
        Fetcher fetcher = new Fetcher(msg);
        String serverName = ChannelManager.getInstance(ChannelType.COORDINATE)
                                          .getChannelName(channel);

        logger.info("收到来自协调节点 {} 的 Fetch 请求 {} ", serverName, fetcher.getGAO());
        try {
            FetchDataInfo fetchDataInfo = LogManager.getINSTANCE()
                                                    .getAfter(fetcher.getGAO());

            FetchResponse fetchResponse = new FetchResponse(fetchDataInfo);
            send(serverName, fetchResponse, RequestProcessor.REQUIRE_NESS());
        } catch (IOException e) {
            throw new HanabiException("Fetch " + fetcher.getGAO() + " 之后的消息失败");
        }
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
                //                logger.debug("正在创建向 {} 发送 {} 的任务", serverName, typeEnum.name());
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

            if (!requestProcessor.isComplete()) {

                CoordinateSender.send(serverName, command);

                if (RequestAndResponseType.get(operationTypeEnum) == null) {
                    // 是不需要回复的类型
                    requestProcessor.complete();
                    writeLockSupplier(() -> inFlight.compute(serverName, (s, enums) -> {
                            if (enums == null) {
                                enums = new HashMap<>();
                            }
                            enums.remove(operationTypeEnum);
                            return enums;
                        })
                    );
                } else {
                    //                    TimedTask task = new TimedTask(1000, () -> sendImpl(serverName, command, requestProcessor, operationTypeEnum));
                    //
                    //                    inFlight.get(serverName)
                    //                            .get(operationTypeEnum)
                    //                            .registerTask(task);
                    //
                    //                    Timer.getInstance()// 扔进时间轮不断重试，直到收到此消息的回复
                    //                         .addTask(task);
                }
            }
            return null;
        });
    }
}
