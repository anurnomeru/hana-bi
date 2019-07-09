package com.anur.core.coordinate.apis;

import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.CoordinateConfigHelper;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.coordinate.model.RequestProcessor;
import com.anur.core.coordinate.operator.CoordinateClientOperator;
import com.anur.core.elect.ElectMeta;
import com.anur.core.listener.EventEnum;
import com.anur.core.listener.HanabiListener;
import com.anur.core.lock.ReentrantReadWriteLocker;
import com.anur.core.struct.coordinate.FetchResponse;
import com.anur.core.struct.coordinate.Fetcher;
import com.anur.io.store.prelog.ByteBufPreLogManager;
import com.anur.timewheel.TimedTask;
import com.anur.timewheel.Timer;

/**
 * Created by Anur IjuoKaruKas on 2019/3/26
 *
 * 日志一致性控制器
 */
public class CoordinateApisManager extends ReentrantReadWriteLocker {

    private static volatile CoordinateApisManager INSTANCE;



    public static CoordinateApisManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (CoordinateApisManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new CoordinateApisManager();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * 如何消费 Fetch response
     */
    private BiConsumer<String, FetchResponse> CONSUME_FETCH_RESPONSE = (node, fetchResponse) -> {
        readLockSupplier(() -> {
            logger.debug("收到节点 {} 返回的 FETCH_RESPONSE", node);

            if (ElectMeta.INSTANCE.isLeader()) {
                logger.error("出现了不应该出现的情况！");
            }

            if (fetchResponse.getFileOperationSetSize() == 0) {
                return null;
            }

            ByteBufPreLogManager.getINSTANCE()
                                .append(fetchResponse.getGeneration(), fetchResponse.read());
            return null;
        });
    };

    public CoordinateApisManager() {
        HanabiListener.INSTANCE.register(EventEnum.CLUSTER_VALID,
            () -> {
                if (!ElectMeta.INSTANCE.isLeader()) {
                    writeLockSupplier(() -> {
                        CoordinateClientOperator client = CoordinateClientOperator.getInstance(InetSocketAddressConfigHelper.getNode(ElectMeta.INSTANCE.getLeader()));

                        // 如果节点非Leader，需要连接 Leader，并创建 Fetch 定时任务
                        // 当集群可用时，连接协调 leader

                        client.registerWhenConnectToLeader(() -> {
                            fetchLock.lock();
                            try {
                                rebuildFetchTask(cvc, ElectMeta.INSTANCE.getLeader());
                            } finally {
                                fetchLock.unlock();
                            }
                        });

                        client.registerWhenDisconnectToLeader(() -> {
                            fetchLock.lock();
                            try {
                                cancelFetchTask();
                                cvc++;
                            } finally {
                                fetchLock.unlock();
                            }
                        });
                        client.tryStartWhileDisconnected();
                        return null;
                    });
                }
                return null;
            }
        );

        HanabiListener.INSTANCE.register(EventEnum.CLUSTER_INVALID,
            () ->
                writeLockSupplier(() -> {
                    ApisManager.getINSTANCE()
                               .reboot();

                    cvc++;
                    cancelFetchTask();

                    // 当集群不可用时，与协调 leader 断开连接
                    CoordinateClientOperator.shutDownInstance("集群已不可用，与协调 Leader 断开连接");
                    return null;
                })
        );
    }

    private void cancelFetchTask() {
        Optional.ofNullable(fetchPreLogTask)
                .ifPresent(TimedTask::cancel);
        logger.debug("取消 FetchPreLog 定时任务");
    }

    private void rebuildFetchTask(long myVersion, String fetchFrom) {
        if (cvc > myVersion) {
            logger.debug("sendFetchPreLog Task is out of version.");
            return;
        }

        fetchPreLogTask = new TimedTask(CoordinateConfigHelper.getFetchBackOfMs(), () -> sendFetchPreLog(myVersion, fetchFrom));
        Timer.getInstance()
             .addTask(fetchPreLogTask);
        logger.debug("载入 FetchPreLog 定时任务");
    }

    /**
     * 定时 Fetch 消息
     */
    public void sendFetchPreLog(long myVersion, String fetchFrom) {
        fetchLock.lock();
        try {
            Optional.ofNullable(fetchPreLogTask)
                    .filter(fetchTask -> !fetchTask.isCancel())
                    .ifPresent(fetchTask -> {
                        if (ApisManager.getINSTANCE()
                                       .send(
                                           fetchFrom,
                                           new Fetcher(
                                               ByteBufPreLogManager.getINSTANCE()
                                                                   .getPreLogGAO()
                                           ),
                                           new RequestProcessor(byteBuffer ->
                                               CONSUME_FETCH_RESPONSE.accept(fetchFrom, new FetchResponse(byteBuffer)),
                                               () -> rebuildFetchTask(myVersion, fetchFrom)))) {
                        }
                    });
        } finally {
            fetchLock.unlock();
        }
    }
}
