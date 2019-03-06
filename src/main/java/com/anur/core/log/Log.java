package com.anur.core.log;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.LogConfigHelper;
import com.anur.core.lock.ReentrantLocker;
import com.anur.core.log.operation.Operation;
import com.google.common.collect.Lists;

/**
 * Created by Anur IjuoKaruKas on 2019/3/6
 */
public class Log extends ReentrantLocker {

    private static Logger logger = LoggerFactory.getLogger(Log.class);

    /** 当前目录所处世代 */
    public final long generation;

    /** 日志文件相关目录，目录以世代命名 */
    public final File dir;

    /** 管理这个世代下所有的日志分片文件 */
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();

    /** 此 offset 之前的数据都已经刷盘 */
    public long recoveryPoint = 0L;

    /** 最近一个 append 到日志文件中的 offset */
    private long currentOffset = 0L;

    public Log(long generation, File dir, long recoveryPoint) {
        this.generation = generation;
        this.dir = dir;
        this.recoveryPoint = recoveryPoint;
    }

    /**
     * 将一个操作添加到日志文件中
     */
    public void append(Operation operation) {
        LogSegment logSegment =
    }

    public LogSegment maybeRoll(int size) {
        LogSegment logSegment = activeSegment();
        if (logSegment.size() + size > LogConfigHelper.getMaxLogSegmentSize() || logSegment.getOffsetIndex()
                                                                                           .isFull()) {
            logger.info("即将开启新的日志分片，上个分片大小为 {}/{}， 对应的索引文件共建立了 {}/{} 个索引。", logSegment.size(), LogConfigHelper.getMaxLogSegmentSize(),
                logSegment.getOffsetIndex()
                          .getEntries(), logSegment.getOffsetIndex()
                                                   .getMaxEntries());
            return
        }
    }

    /**
     * 获取最后一个日志分片文件
     */
    private LogSegment activeSegment() {
        return segments.lastEntry()
                       .getValue();
    }

    /**
     * 滚动到下一个日志分片文件
     */
    private LogSegment roll() {
        return this.lockSupplier(() -> {
            long newOffset = currentOffset + 1;
            File newFile = LogCommon.logFilename(dir, newOffset);
            File indexFile = LogCommon.indexFilename(dir, newOffset);

            Lists.newArrayList(newFile, indexFile)
                 .forEach(file -> Optional.of(newFile)
                                          .filter(File::exists)
                                          .ifPresent(f -> {
                                              logger.info("新创建的日志分片或索引竟然已经存在，将其抹除。");
                                              f.delete();
                                          }));


        });
    }
}
