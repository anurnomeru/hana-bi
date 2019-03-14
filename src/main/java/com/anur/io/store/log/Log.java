package com.anur.io.store.log;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.config.LogConfigHelper;
import com.anur.core.lock.ReentrantLocker;
import com.anur.io.store.common.LogCommon;
import com.anur.io.store.operationset.ByteBufferOperationSet;
import com.anur.io.store.common.Operation;
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

    /** 最近一个添加到 append 到日志文件中的 offset */
    private long currentOffset = 0L;

    public Log(long generation, File dir, long recoveryPoint) throws IOException {
        this.generation = generation;
        this.dir = dir;
        this.recoveryPoint = recoveryPoint;
        load();
    }

    private void load() throws IOException {
        // 如果目录不存在，则创建此目录
        dir.mkdirs();
        logger.info("正在读取操作日志目录 {}", dir.getName());

        for (File file : dir.listFiles()) {
            if (file.isFile()) {

                if (!file.canRead()) {
                    throw new HanabiException("日志分片文件或索引文件不可读！");
                }

                String filename = file.getName();
                if (filename.endsWith(LogCommon.IndexFileSuffix)) {
                    File logFile = new File(file.getAbsolutePath()
                                                .replace(LogCommon.IndexFileSuffix, LogCommon.LogFileSuffix));

                    if (!logFile.exists()) {
                        logger.warn("日志索引文件 {} 被创建了，但并没有创建相应的日志切片文件", filename);
                        file.delete();
                        break;
                    }
                } else if (filename.endsWith(LogCommon.LogFileSuffix)) {
                    long start = Long.valueOf(filename.substring(0, filename.length() - LogCommon.LogFileSuffix.length()));
                    File indexFile = LogCommon.indexFilename(dir, start);
                    LogSegment thisSegment;
                    try {
                        thisSegment = new LogSegment(dir, start, LogConfigHelper.getIndexInterval(), LogConfigHelper.getMaxIndexSize());
                    } catch (IOException e) {
                        throw new HanabiException("创建或映射日志分片文件 " + filename + " 失败");
                    }

                    if (indexFile.exists()) {
                        // 检查下这个索引文件有没有大的问题，比如大小不正确等
                        try {
                            thisSegment.getOffsetIndex()
                                       .sanityCheck();
                        } catch (Exception e) {
                            logger.info("日志 {} 的索引文件存在异常，正在重建索引文件。", filename);
                            thisSegment.recover(LogConfigHelper.getMaxLogMessageSize());
                        }
                    } else {
                        logger.info("日志 {} 的索引文件不存在，正在重建索引文件。", filename);
                        thisSegment.recover(LogConfigHelper.getMaxLogMessageSize());
                    }

                    segments.put(start, thisSegment);
                }
            }
        }

        if (segments.size() == 0) {
            logger.info("当前目录 {} 还未创建任何日志分片，将创建开始下标为 1L 的日志分片", dir.getAbsolutePath());
            segments.put(0L, new LogSegment(dir, 1, LogConfigHelper.getIndexInterval(), LogConfigHelper.getMaxIndexSize()));
        } else {
            currentOffset = activeSegment().lastOffset();
        }
    }

    /**
     * 最近加入到日志中的那个 offset
     */
    public long getCurrentOffset() {
        return currentOffset;
    }

    /**
     * 将一个操作添加到日志文件中
     */
    public void append(Operation operation, long offset) {
        if (offset < currentOffset) {
            throw new HanabiException("一定是哪里有问题");
        }

        LogSegment logSegment = maybeRoll(operation.size());

        ByteBufferOperationSet byteBufferOperationSet = new ByteBufferOperationSet(operation, offset);
        try {
            logSegment.append(offset, byteBufferOperationSet);
        } catch (IOException e) {
            throw new HanabiException("写入操作日志失败：" + operation.toString());
        }
        currentOffset = offset;
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
