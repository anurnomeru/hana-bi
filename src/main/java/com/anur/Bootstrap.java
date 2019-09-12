package com.anur;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.InetSocketAddressConfiguration;
import com.anur.core.coordinate.apis.fetch.LeaderCoordinateManager;
import com.anur.core.coordinate.apis.recovery.FollowerClusterRecoveryManager;
import com.anur.core.coordinate.apis.recovery.LeaderClusterRecoveryManager;
import com.anur.core.coordinate.apis.fetch.FollowerCoordinateManager;
import com.anur.core.coordinate.operator.CoordinateServerOperator;
import com.anur.core.elect.operator.ElectOperator;
import com.anur.core.elect.operator.ElectServerOperator;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.Operation;
import com.anur.core.util.HanabiExecutors;
import com.anur.io.store.log.LogManager;
import com.anur.io.store.log.CommitProcessManager;

/**
 * Created by Anur IjuoKaruKas on 2019/3/13
 */
public class Bootstrap {

    private static Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    private static volatile boolean RUNNING = true;

    public static void main(String[] args) throws InterruptedException {

        logger.info(
            "\n\n" +
                " _     _                   _     _ \n" +
                "| |   | |                 | |   (_)\n" +
                "| |__ | | ____ ____   ____| | _  _ \n" +
                "|  __)| |/ _  |  _ \\ / _  | || \\| |\n" +
                "| |   | ( ( | | | | ( ( | | |_) ) |\n" +
                "|_|   |_|\\_||_|_| |_|\\_||_|____/|_|\n" +
                "           Hanabi     (ver 0.0.1)\n\n" +
                "节点 - " + InetSocketAddressConfiguration.Companion.getServerName() + "\n");

        HanabiExecutors.Companion.execute(() -> {

            try {
                /*
                 * 日志一致性控制器
                 */
                FollowerCoordinateManager forInitial01 = FollowerCoordinateManager.INSTANCE;

                /*
                 * 日志一致性控制器
                 */
                LeaderCoordinateManager forInitial02 = LeaderCoordinateManager.INSTANCE;

                /*
                 * 集群日志恢复器
                 */
                LeaderClusterRecoveryManager forInitial03 = LeaderClusterRecoveryManager.INSTANCE;

                /*
                 * 集群日志恢复器
                 */
                FollowerClusterRecoveryManager forInitial04 = FollowerClusterRecoveryManager.INSTANCE;

                /*
                 * 提交记录管理者（仅leader）
                 */
                CommitProcessManager forInitial05 = CommitProcessManager.INSTANCE;

                /*
                 * 初始化日志管理
                 */
                LogManager logManager = LogManager.INSTANCE;

                /*
                 * 启动协调服务器
                 */
                CoordinateServerOperator.getInstance()
                                        .start();

                /*
                 * 启动选举服务器，没什么主要的操作，这个服务器主要就是应答选票以及应答成为 Flower 用
                 */
                ElectServerOperator.getInstance()
                                   .start();

                /*
                 * 启动选举客户端，初始化各种投票用的信息，以及启动成为候选者的定时任务
                 */
                ElectOperator.getInstance()
                             .start();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }

            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                for (int i = 0; i < 10000000; i++) {
                    Operation operation = new Operation(OperationTypeEnum.SETNX,
                        "ToIjuoKaruKassetAnurToIjuoKaruKas",
                        "ToIjuoKaruKasToIjuoKaruKasToIjuoKaruKasToIjuoKaruKasToIjuoKaruKasToIjuoKaruKasToIjuoKaruKasToIjuoKaruKasToIjuoKaruKasToIjuoKaruKasToIjuoKaruKasToIjuoKaruKasToIjuoKaruKas");
                    LogManager.INSTANCE.appendUntilClusterValid(operation);
                }

                System.out.println("zzzzz");
            } catch (Exception e) {
            }
        });

        while (RUNNING) {
            Thread.sleep(1000);
        }
    }
}
