package com.anur.core;

import java.util.List;
import java.util.Map;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.coordinate.CoordinateClientOperator;
import com.anur.core.elect.ElectOperator;
import com.anur.core.lock.ReentrantReadWriteLocker;

/**
 * Created by Anur IjuoKaruKas on 2019/3/26
 *
 * 日志一致性控制器
 */
public class ConsistentManager extends ReentrantReadWriteLocker {

    private static volatile ConsistentManager INSTANCE;

    private volatile boolean clusterValid;

    public static ConsistentManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (ConsistentManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ConsistentManager();
                }
            }
        }
        return INSTANCE;
    }

    public ConsistentManager() {

        ElectOperator.getInstance()
                     .registerWhenClusterValid(cluster -> writeLockSupplier(() -> {
                         clusterValid = true;
                         CoordinateClientOperator.getInstance(InetSocketAddressConfigHelper.getNode(cluster.getLeader()))
                                                 .tryStartWhileDisconnected();
                         System.err.println("触发集群可用啦");
                         return null;
                     }));

        ElectOperator.getInstance()
                     .registerWhenClusterInvalid(() -> writeLockSupplier(() -> {
                         clusterValid = false;
                         CoordinateClientOperator.shutDownInstance("集群已不可用，与协调 Leader 断开连接");
                         System.err.println("触发集群不可用啦");
                         return null;
                     }));
    }
}
