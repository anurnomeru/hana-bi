package com.anur.core.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.coordinate.model.Response;
import com.anur.core.lock.ReentrantLocker;
import com.anur.core.util.ChannelManager.ChannelType;
import com.anur.core.command.core.Operation;
import com.anur.core.command.common.OperationTypeEnum;

/**
 * Created by Anur IjuoKaruKas on 2019/3/27
 *
 * 此管理器负责消息的重发、并保证这种消息类型，在收到回复之前，无法继续发同一种类型的消息
 *
 * 1、消息在没有收到回复之前，会定时重发。
 * 2、那么如何保证数据不被重复消费：我们以时间戳作为 key 的一部分，应答方需要在消费消息后，需要记录此时间戳，并不再消费比此时间戳小的消息。
 */
public class InFlightRequestManager extends ReentrantLocker {

    private static final Logger LOGGER = LoggerFactory.getLogger(InFlightRequestManager.class);

    private Map<String, Set<OperationTypeEnum>> inFlight;

    public boolean send(String serverName, Operation operation, Response response) {
        OperationTypeEnum typeEnum = operation.getOperationTypeEnum();
        return this.lockSupplier(() -> {

            if (Optional.ofNullable(inFlight.get(serverName))
                        .map(enums -> enums.contains(typeEnum))
                        .orElse(false)) {

                LOGGER.debug("尝试创建发送到节点 {} 的 {} 任务失败，上次的指令还未收到回复", serverName, typeEnum.name());
                return false;
            } else {
                LOGGER.debug("尝试创建发送到节点 {} 的 {} 任务成功", serverName, typeEnum.name());

                inFlight.compute(serverName, (s, enums) -> {
                    if (enums == null) {
                        enums = new HashSet<>();
                    }
                    enums.add(typeEnum);
                    return enums;
                });

                return true;
            }
        });
    }

    /**
     * 真正发送消息的方法，内置了重发机制
     */
    private void sendImpl(String serverName, Operation operation, Response response) {
        if (!response.isComplate()) {
            ChannelManager.getInstance(ChannelType.COORDINATE)
                          .getChannel(serverName);


        }
    }
}
