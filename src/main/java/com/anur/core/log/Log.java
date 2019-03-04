package com.anur.core.log;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Anur IjuoKaruKas on 2019/3/1
 */
public class Log {

    private static Logger logger = LoggerFactory.getLogger(Log.class);

    public Log(File dir, long generation, long recoveryPoint) {
        this.dir = dir;
        this.generation = generation;
        this.recoveryPoint = recoveryPoint;
    }

    /**
     * The directory in which log segments are created.
     */
    private final File dir;

    /**
     * the actual segments of the log
     */
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();

    /**
     * TODO 疑问：既然是要实时计算，为啥定义成var？
     *
     * Calculate the offset of the next message
     */
    private volatile LogOffsetMetadata nextOffsetMetadata = new LogOffsetMetadata(activeSegment().nextOffset(), activeSegment().getBaseOffset(), (int) activeSegment().size());

    /**
     * Log segments is named by generation
     */
    private long generation;

    /**
     * The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
     */
    private volatile long recoveryPoint = 0;

    private void leadSements() {
        // 如果不存在的话，先创建这个目录
        dir.mkdirs();

        Set<File> swapFiles = new HashSet<>();
        File[] files = dir.listFiles();
        //        for (File file : files) {
        //            if (file.isFile()) {
        //
        //                if (!file.canRead()) {
        //                    throw new HanabiException("Could not read file " + file);
        //                }
        //
        //                String fileName = file.getName();
        //                if (fileName.endsWith(LogConstant.SwapFileSuffix)) {
        //
        //                }
        //            }
        //        }

        for (File file : files) {
            if (file.isFile()) {
                String fileName = file.getName();

                // 确保索引文件有相应的操作日志文件
                if (fileName.endsWith(LogCommon.IndexFileSuffix)) {
                    File logFile = new File(file.getAbsolutePath()
                                                .replace(LogCommon.IndexFileSuffix, LogCommon.LogFileSuffix));
                    if (!logFile.exists()) {
                        file.delete();
                        logger.error("索引文件 {} 没有对应的日志文件，已将其删除。", fileName);
                    }
                } else if (fileName.endsWith(LogCommon.LogFileSuffix)) {
                    File indexFile = new File(file.getAbsolutePath()
                                                  .replace(LogCommon.LogFileSuffix, LogCommon.IndexFileSuffix));
                    LogSegment logSegment = new LogSegment();
                }
            }
        }
    }

    /**
     * Calculate the offset of the next message
     */
    //    private volatile LogOffsetMetadata nextOffsetMetadata = new LogOffsetMetadata();
    private LogSegment activeSegment() {
        return segments.lastEntry()
                       .getValue();
    }

    public String name() {
        return dir.getName();
    }
}
