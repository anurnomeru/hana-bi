package com.anur.io.store.log;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.LogConfigHelper;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.lock.ReentrantLocker;
import com.anur.core.struct.base.Operation;
import com.anur.core.util.HanabiExecutors;
import com.anur.exception.LogException;
import com.anur.io.store.common.LogCommon;
import com.anur.io.store.common.OperationAndOffset;
import com.anur.io.store.common.PreLogMeta;
import com.anur.io.store.operationset.ByteBufferOperationSet;
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
    public long recoveryPoint;

    /** 最近一个添加到 append 到日志文件中的 offset，默认取 baseOffset */
    private long currentOffset;

    public Log(long generation, File dir) throws IOException {
        this.generation = generation;
        this.dir = dir;
        this.currentOffset = load();
        this.recoveryPoint = currentOffset;
    }

    private long load() throws IOException {
        // 如果目录不存在，则创建此目录
        dir.mkdirs();
        logger.info("正在读取日志文件目录 {}", dir.getName());

        for (File file : dir.listFiles()) {
            if (file.isFile()) {

                if (!file.canRead()) {
                    throw new LogException("日志分片文件或索引文件不可读！");
                }

                String filename = file.getName();
                if (filename.endsWith(LogCommon.Companion.getIndexFileSuffix())) {
                    File logFile = new File(file.getAbsolutePath()
                                                .replace(LogCommon.Companion.getIndexFileSuffix(), LogCommon.Companion.getLogFileSuffix()));

                    if (!logFile.exists()) {
                        logger.warn("世代 {} 日志索引文件 {} 被创建了，但并没有创建相应的日志切片文件", generation, filename);
                        file.delete();
                        break;
                    }
                } else if (filename.endsWith(LogCommon.Companion.getLogFileSuffix())) {
                    long start = Long.valueOf(filename.substring(0, filename.length() - LogCommon.Companion.getLogFileSuffix()
                                                                                                           .length()));
                    File indexFile = LogCommon.Companion.indexFilename(dir, start);
                    LogSegment thisSegment;
                    try {
                        thisSegment = new LogSegment(dir, start, LogConfigHelper.getIndexInterval(), LogConfigHelper.getMaxIndexSize());
                    } catch (IOException e) {
                        throw new LogException("创建或映射日志分片文件 " + filename + " 失败");
                    }

                    if (indexFile.exists()) {
                        // 检查下这个索引文件有没有大的问题，比如大小不正确等
                        try {
                            thisSegment.getOffsetIndex()
                                       .sanityCheck();
                        } catch (Exception e) {
                            logger.info("世代 {} 日志 {} 的索引文件存在异常，正在重建索引文件。", generation, filename);
                            indexFile.delete();
                            thisSegment.recover(LogConfigHelper.getMaxLogMessageSize());
                        }
                    } else {
                        logger.info("世代 {} 日志 {} 的索引文件不存在，正在重建索引文件。", generation, filename);
                        thisSegment.recover(LogConfigHelper.getMaxLogMessageSize());
                    }

                    segments.put(start, thisSegment);
                }
            }
        }

        if (segments.size() == 0) {
            logger.info("当前世代 {} 目录 {} 还未创建任何日志分片，将创建开始下标为 1L 的日志分片", generation, dir.getAbsolutePath());
            segments.put(0L, new LogSegment(dir, 0, LogConfigHelper.getIndexInterval(), LogConfigHelper.getMaxIndexSize()));
        }

        return activeSegment().lastOffset(generation);
    }

    /**
     * 最近加入到日志中的那个 offset
     */
    public long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    /**
     * 将一个操作添加到日志文件中
     */
    public void append(Operation operation, long offset) {
        if (offset < currentOffset) {
            throw new LogException("一定是哪里有问题");
        }

        LogSegment logSegment = maybeRoll(operation.size());

        ByteBufferOperationSet byteBufferOperationSet = new ByteBufferOperationSet(operation, offset);
        try {
            logSegment.append(offset, byteBufferOperationSet);
        } catch (IOException e) {
            throw new LogException("写入日志文件失败：" + operation.toString());
        }
        currentOffset = offset;
    }

    /**
     * 将多个操作添加到日志文件中
     */
    public void append(PreLogMeta preLogMeta, long startOffset, long endOffset) {
        if (endOffset < currentOffset) {
            throw new LogException(String.format("一定是哪里有问题，追加日志文件段 start：%s end：%s，但当前 current：%s", startOffset, endOffset, currentOffset));
        } else {
            logger.info(String.format("追加日志文件段 start：%s end：%s 准备，当前 current：%s", startOffset, endOffset, currentOffset));
        }

        int count = 0;

        try {
            for (OperationAndOffset operationAndOffset : preLogMeta.getOao()) {
                count++;
                long offset = operationAndOffset.getOffset();
                if (offset > currentOffset) {
                    continue;
                }
                append(operationAndOffset.getOperation(), offset);
            }
        } catch (Throwable e) {
            throw new LogException("写入日志文件失败：" + startOffset + " => " + endOffset + " " + e.getMessage());
        }
        currentOffset = endOffset;
        logger.info(String.format("追加日志文件段 start：%s end：%s 完毕，当前 current：%s，共追加 %s 条操作日志", startOffset, endOffset, currentOffset, count));
    }

    /**
     * 如果要 append 的日志过大，则需要滚动到下一个日志分片，如果滚动到下一个日志分片，则将上一个日志文件刷盘。
     */
    public LogSegment maybeRoll(int size) {
        LogSegment logSegment = activeSegment();

        if (
            logSegment.size() + size > LogConfigHelper.getMaxLogSegmentSize() // 即将 append 的消息将超过分片容纳最大大小
                || logSegment.getOffsetIndex() // 可索引的 index 已经达到最大
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
            File newFile = LogCommon.Companion.logFilename(dir, newOffset);
            File indexFile = LogCommon.Companion.indexFilename(dir, newOffset);

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
                throw new LogException("滚动时创建新的日志分片失败");
            }

            if (addSegment(newLogSegment) != null) {
                logger.error("滚动时创建新的日志分片失败，该分片已经存在");
            }

            flush(newOffset);
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
    public void flush(long offset) {
        if (offset <= recoveryPoint) {
            return;
        }

        logger.debug("将世代 {} 日志 {} 刷盘，现刷盘至 offset {}，上次刷盘时间为 {}，现共有 {} 条消息还未刷盘", generation, name(), offset, lastFlushedTime.get(), unFlushedMessages());
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
     * 如果这个世代下所有的日志分片文件不存在小于等于 fromOffset 的
     *
     * - 返回从头直到 toOffset 的 LogSegment
     *
     * - 否则返回区间内的 logSegment
     */
    public Iterable<LogSegment> getLogSegments(long fromOffset, long toOffset) {
        return lockSupplier(() -> {

            // 返回的最大键，小于或等于给定的键
            // floorKey(K key) 方法用于返回的最大键小于或等于给定的键，或null，
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
