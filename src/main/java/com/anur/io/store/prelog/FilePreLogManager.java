package com.anur.io.store.prelog;

import java.io.File;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.io.store.common.Operation;
import com.anur.io.store.log.Log;
import com.anur.io.store.log.LogSegment;
import com.anur.io.store.manager.LogManager;
import io.netty.buffer.ByteBuf;

/**
 * Created by Anur IjuoKaruKas on 2019/3/22
 */
public class FilePreLogManager extends LogManager implements PreLogger {

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

    @Override
    public ByteBuf getAfter(GenerationAndOffset GAO) {
        long gen = GAO.getGeneration();
        long offset = GAO.getOffset();

        ConcurrentNavigableMap<Long, Log> tail = generationDirs.tailMap(gen, true);
        for (Entry<Long, Log> entry : tail.entrySet()) {
            Iterable<LogSegment> logSegments = entry.getValue()
                                                    .getLogSegments(offset, Long.MAX_VALUE);
        }

        return null;
    }

    @Override
    public ByteBuf getBefore(GenerationAndOffset GAO) {
        long gen = GAO.getGeneration();
        long offset = GAO.getOffset();
        return null;
    }

    @Override
    public void discardBefore(GenerationAndOffset GAO) {
        long gen = GAO.getGeneration();
        long offset = GAO.getOffset();
    }
}
