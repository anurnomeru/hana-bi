package com.anur.core.util;

import java.util.Map;
import java.util.Set;
import com.anur.io.store.common.Operation;
import com.anur.io.store.common.OperationTypeEnum;

/**
 * Created by Anur IjuoKaruKas on 2019/3/27
 *
 * 此管理器负责消息的重发、并保证这种消息类型，在收到回复之前，无法继续发同一种类型的消息
 *
 * 1、消息在没有收到回复之前，会定时重发。
 * 2、那么如何保证数据不被重复消费：我们以时间戳作为 key 的一部分，应答方需要在消费消息后，需要记录此时间戳，并不再消费比此时间戳小的消息。
 */
public class InFlightRequestManager {

    private Map<String, Set<OperationTypeEnum>> inFlight;

    public void send(String serverName, Operation operation) {
        OperationTypeEnum typeEnum = operation.getOperationTypeEnum();
    }
}
