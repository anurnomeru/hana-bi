package com.anur.core.log;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import com.anur.core.coordinate.CoordinateServerOperator;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.ElectServerOperator;
import com.anur.core.elect.model.GennerationAndOffset;
import com.anur.core.log.common.LogCommon;
import com.anur.core.log.common.Operation;
import com.anur.core.log.common.OperationTypeEnum;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2019/3/13
 */
public class LogManager {

    private static final String LogDir = "\\store\\aof";

    public static volatile LogManager INSTANCE;

    private final ConcurrentSkipListMap<Long, Log> generationDirs;

    private final File baseDir;

    private final GennerationAndOffset initial;

    public static void main(String[] args) throws InterruptedException {
        LogManager l = getINSTANCE();

        /**
         * 启动协调服务器
         */
        CoordinateServerOperator.getInstance()
                                .start();

        /**
         * 启动选举服务器，没什么主要的操作，这个服务器主要就是应答选票以及应答成为 Flower 用
         */
        ElectServerOperator.getInstance()
                           .start();

        /**
         * 启动选举客户端，初始化各种投票用的信息，以及启动成为候选者的定时任务
         */
        ElectOperator.getInstance()
                     .resetGenerationAndOffset(l.getInitial())
                     .start();

        Thread.sleep(10000);

        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            Operation operation = new Operation(OperationTypeEnum.SETNX, random.nextLong() + "", random.nextLong() + "");
            l.append(operation);
        }
    }

    public static LogManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (LogManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new LogManager();
                }
            }
        }
        return INSTANCE;
    }

    private LogManager() {
        this.generationDirs = new ConcurrentSkipListMap<>();
        String relativelyPath = System.getProperty("user.dir");
        this.baseDir = new File(relativelyPath + LogDir);
        initial = load();
    }

    /**
     * 加载既存的目录们
     */
    private GennerationAndOffset load() {
        baseDir.mkdirs();

        long latestGeneration = 0L;

        for (File file : baseDir.listFiles()) {
            if (!file.isFile()) {
                latestGeneration = Math.max(latestGeneration, Integer.valueOf(file.getName()));
            }
        }

        GennerationAndOffset init;

        // 只要创建最新的那个 generation 即可
        try {
            Log latest = new Log(latestGeneration, createGenDirIfNEX(latestGeneration), 0);
            generationDirs.put(1L, latest);
            init = new GennerationAndOffset(latestGeneration, latest.getCurrentOffset());
        } catch (IOException e) {
            throw new HanabiException("操作日志初始化失败，项目无法启动");
        }

        return init;
    }

    /**
     * 添加一条操作日志到磁盘的入口
     */
    public void append(Operation operation) {
        GennerationAndOffset operationId = ElectOperator.getInstance()
                                                        .genOperationId();

        Log log = maybeRoll(operationId.getGeneration());
        log.append(operation, operationId.getOffset());
    }

    /**
     * 在 append 操作时，如果世代更新了，则创建新的 Log 管理
     */
    private Log maybeRoll(long generation) {
        Log current = activeLog();
        if (generation > current.generation) {
            File dir = createGenDirIfNEX(generation);
            Log log;
            try {
                log = new Log(generation, dir, 0);
            } catch (IOException e) {
                throw new HanabiException("创建世代为 " + generation + " 的操作日志管理文件 Log 失败");
            }

            generationDirs.put(generation, log);
            return log;
        } else if (generation < current.generation) {
            throw new HanabiException("不应在添加日志时获取旧世代的 Log");
        }

        return current;
    }

    /**
     * 创建世代目录
     */
    private File createGenDirIfNEX(long generation) {
        return LogCommon.dirName(baseDir, generation);
    }

    /**
     * 获取最新的一个日志分片管理类 Log
     */
    private Log activeLog() {
        return generationDirs.lastEntry()
                             .getValue();
    }

    public GennerationAndOffset getInitial() {
        return initial;
    }
}
