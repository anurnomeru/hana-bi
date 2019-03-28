package com.anur.io.coordinate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.command.core.AbstractCommand;
import com.anur.core.coordinate.model.Response;
import com.anur.core.command.common.OperationTypeEnum;
import com.anur.core.lock.ReentrantReadWriteLocker;
import com.anur.io.core.coder.CoordinateSender;
import com.anur.timewheel.TimedTask;
import com.anur.timewheel.Timer;

/**
 * Created by Anur IjuoKaruKas on 2019/3/27
 *
 * 此管理器负责消息的重发、并保证这种消息类型，在收到回复之前，无法继续发同一种类型的消息
 *
 * 1、消息在没有收到回复之前，会定时重发。
 * 2、那么如何保证数据不被重复消费：我们以时间戳作为 key 的一部分，应答方需要在消费消息后，需要记录此时间戳，并不再消费比此时间戳小的消息。
 */
public class InFlightRequestManager extends ReentrantReadWriteLocker {

    private static volatile InFlightRequestManager INSTANCE;

    public static InFlightRequestManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (InFlightRequestManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new InFlightRequestManager();
                }
            }
        }
        return INSTANCE;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(InFlightRequestManager.class);

    private Map<String, Set<OperationTypeEnum>> inFlight = new HashMap<>();

    /**
     * 重启此类，用于在重新选举后，刷新所有任务，不再执着于上个世代的任务
     */
    private void reboot() {
        this.writeLockSupplier(() -> {

            return null;
        });
    }

    /**
     * 此发送器保证【一个类型的消息】只能在收到回复前发送一次，类似于仅有 1 容量的Queue
     */
    public boolean send(String serverName, AbstractCommand command, Response response) {
        OperationTypeEnum typeEnum = command.getOperationTypeEnum();
        return this.writeLockSupplier(() -> {

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

                sendImpl(serverName, command, response);

                return true;
            }
        });
    }

    /**
     * 真正发送消息的方法，内置了重发机制
     */
    private void sendImpl(String serverName, AbstractCommand command, Response response) {
        this.readLockSupplier(() -> {
            if (!response.isComplete()) {
                CoordinateSender.send(serverName, command.getByteBuffer(), command.size());

                TimedTask task = new TimedTask(1000, () -> sendImpl(serverName, command, response));

                Timer.getInstance()// 扔进时间轮不断重试，直到收到此消息的回复
                     .addTask(task);
            }
            return null;
        });
    }
}
