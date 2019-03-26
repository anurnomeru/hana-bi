package com.anur.io.store.prelog;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentNavigableMap;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.exception.HanabiException;
import com.anur.io.store.common.FetchDataInfo;
import com.anur.io.store.common.Operation;
import com.anur.io.store.log.Log;
import com.anur.io.store.log.LogSegment;
import com.anur.io.store.manager.LogManager;

/**
 * Created by Anur IjuoKaruKas on 2019/3/22
 */
public class FilePreLogManager extends LogManager {

    public static volatile FilePreLogManager INSTANCE;

    public static LogManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (LogManager.class) {
                if (INSTANCE == null) {
                    String relativelyPath = System.getProperty("user.dir");
                    INSTANCE = new FilePreLogManager(new File(relativelyPath + "\\" + InetSocketAddressConfigHelper.getServerName() + "\\prelog\\aof\\"));
                }
            }
        }
        return INSTANCE;
    }

    public FilePreLogManager(File path) {
        super(path);
    }

    @Override
    public void append(Operation operation) {
        super.append(operation);
    }

    /**
     * 只返回某个 segment 的往后所有消息，需要客户端轮询拉数据（包括拉取本身这条消息）
     */
    public FetchDataInfo getAfter(GenerationAndOffset GAO) throws IOException {
        long gen = GAO.getGeneration();
        long offset = GAO.getOffset();

        ConcurrentNavigableMap<Long, Log> tail = generationDirs.tailMap(gen, true);
        if (tail == null || tail.size() == 0) {
            throw new HanabiException("获取预日志时：世代过大或者此世代还未有预日志");
        }

        Log needLoad = tail.firstEntry()
                           .getValue();

        Iterable<LogSegment> logSegmentIterable = needLoad.getLogSegments(offset, currentOffset);
        LogSegment logSegment = logSegmentIterable.iterator()
                                                  .next();

        return logSegment.read(offset, currentOffset, Integer.MAX_VALUE);
    }
}
