package com.anur.core.log;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.LogConfigHelper;
import com.anur.core.elect.ElectOperator;
import com.anur.core.elect.model.GennerationAndOffset;
import com.anur.core.lock.ReentrantLocker;
import com.anur.core.log.operation.ByteBufferOperationSet;
import com.anur.core.log.operation.Operation;
import com.anur.core.util.HanabiExecutors;
import com.anur.exception.HanabiException;
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

    /** 上一次进行刷盘的时间 */
    private final AtomicLong lastFlushedTime = new AtomicLong(System.currentTimeMillis());

    /** 此 offset 之前的数据都已经刷盘 */
    public long recoveryPoint = 0L;

    /** 最近一个需要 append 到日志文件中的 offset */
    private long currentOffset = 0L;

    public Log(long generation, File dir, long recoveryPoint) {
        this.generation = generation;
        this.dir = dir;
        this.recoveryPoint = recoveryPoint;
    }

    /**
     * 将一个操作添加到日志文件中
     */
    public void append(Operation operation) throws IOException {
        GennerationAndOffset operationId = ElectOperator.getInstance()
                                                        .genOperationId();

        long offset = operationId.getOffset();

        if (operationId.getGeneration() > this.generation) {
            // TODO 需要触发上级创建新的Log

            return;
        }

        LogSegment logSegment = maybeRoll(operation.size());

        ByteBufferOperationSet byteBufferOperationSet = new ByteBufferOperationSet(operation, offset);
        logSegment.append(offset, byteBufferOperationSet);
    }

    /**
     * 如果要 append 的日志过大，则需要滚动到下一个日志分片，如果滚动到下一个日志分片，则将上一个日志文件刷盘。
     */
    public LogSegment maybeRoll(int size) {
        LogSegment logSegment = activeSegment();
        if (logSegment.size() + size > LogConfigHelper.getMaxLogSegmentSize() || logSegment.getOffsetIndex()
                                                                                           .isFull()) {
            logger.info("即将开启新的日志分片，上个分片大小为 {}/{}， 对应的索引文件共建立了 {}/{} 个索引。", logSegment.size(), LogConfigHelper.getMaxLogSegmentSize(),
                logSegment.getOffsetIndex()
                          .getEntries(), logSegment.getOffsetIndex()
                                                   .getMaxEntries());
            return roll();
        } else {
            return logSegment;
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

            // 将原日志分片进行 trim 处理
            Optional.ofNullable(segments.lastEntry())
                    .ifPresent(e -> {
                        e.getValue()
                         .getOffsetIndex()
                         .trimToValidSize();
                        e.getValue()
                         .getFileOperationSet()
                         .trim();
                    });

            LogSegment newLogSegment;
            try {
                newLogSegment = new LogSegment(dir, newOffset, LogConfigHelper.getIndexInterval(), LogConfigHelper.getMaxIndexSize());
            } catch (IOException e) {
                logger.error("滚动时创建新的日志分片失败，分片目录：{}, 创建的文件为：{}", dir.getAbsolutePath(), newFile.getName());
                throw new HanabiException("滚动时创建新的日志分片失败");
            }

            if (addSegment(newLogSegment) != null) {
                logger.error("滚动时创建新的日志分片失败，该分片已经存在");
            }

            HanabiExecutors.submit(() -> flush(newOffset));

            return newLogSegment;
        });
    }

    /**
     * 将日志纳入跳表来管理
     */
    private LogSegment addSegment(LogSegment segment) {
        return this.segments.put(segment.getBaseOffset(), segment);
    }

    /**
     * 将消息刷盘
     */
    private void flush(long offset) {
        if (offset <= recoveryPoint) {
            return;
        }
        logger.debug("将日志 {} 刷盘，现刷盘至 offset {}，上次刷盘时间为 {}，现共有 {} 条消息还未刷盘。", name(), offset, lastFlushedTime.get(), unFlushedMessages());

        for (LogSegment logSegment : getLogSegments(recoveryPoint, offset)) {
            logSegment.flush();
        }

        lockSupplier(() -> {
            if (offset > this.recoveryPoint) {
                this.recoveryPoint = offset;
                lastFlushedTime.set(System.currentTimeMillis());
            }
            return null;
        });
    }

    public String name() {
        return dir.getName();
    }

    /**
     * 获取从包含 fromOffset 的那个 segment 开始，到包含 to-1（注意，要传最新的那个分片的start才有这个效果） 的那个 segment 结束的所有日志分片。
     * 如果传入的 fromOffset 过小，则返回 从头 - toOffset的所有分片
     */
    public Iterable<LogSegment> getLogSegments(long fromOffset, long toOffset) {
        return lockSupplier(() -> {

            // 返回的最大键，小于或等于给定的键
            Long floor = segments.floorKey(fromOffset);
            if (floor == null) {// 代表 segments 的所有键都大于 fromOffset
                return segments.headMap(toOffset)
                               .values();
            } else {
                return segments.subMap(floor, true, toOffset, false)
                               .values();
            }
        });
    }

    /**
     * 还未完全刷盘的消息数量
     */
    public long unFlushedMessages() {
        return currentOffset - recoveryPoint;
    }
}
