//package com.anur.core.log;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.HashSet;
//import java.util.Optional;
//import java.util.Set;
//import java.util.concurrent.ConcurrentSkipListMap;
//import java.util.concurrent.atomic.AtomicLong;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import com.anur.config.LogConfigHelper;
//import com.anur.core.lock.ReentrantLocker;
//import com.anur.core.log.index.OffsetIndex.OffsetIndexIllegalException;
//import com.anur.core.log.operation.ByteBufferOperationSet;
//import com.anur.core.util.HanabiExecutors;
//import com.anur.exception.HanabiException;
//import com.google.common.collect.Lists;
//
///**
// * Created by Anur IjuoKaruKas on 2019/3/1
// */
//public class LogEx extends ReentrantLocker {
//
//    private static Logger logger = LoggerFactory.getLogger(LogEx.class);
//
//    public LogEx(File dir, long generation, long recoveryPoint) {
//        this.dir = dir;
//        this.generation = generation;
//        this.recoveryPoint = recoveryPoint;
//    }
//
//    /** The directory in which log segments are created */
//    private final File dir;
//
//    /** the actual segments of the log */
//    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
//
//    /** last time it was flushed */
//    private final AtomicLong lastflushedTime = new AtomicLong(System.currentTimeMillis());
//
//    /**
//     * TODO 疑问：既然是要实时计算，为啥定义成var？
//     * Calculate the offset of the next message
//     */
//    private volatile LogOffsetMetadata nextOffsetMetadata = new LogOffsetMetadata(activeSegment().nextOffset(), activeSegment().getBaseOffset(), (int) activeSegment().size());
//
//    /** Log segments named by generation */
//    private long generation;
//
//    /** current offset assign to the operation */
//    private long currentOffset;
//
//    /**
//     * The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
//     * 此点之前的消息已经刷入磁盘
//     */
//    private volatile long recoveryPoint = 0;
//
//    public LogAppendInfo append(ByteBufferOperationSet operationSet) {
//        lockSupplier(() -> {
//            LogSegment logSegment = maybeRoll(operationSet.sizeInBytes());
//            logSegment.append();
//
//        })
//    }
//
//    private void loadSements() throws IOException {
//        // 如果不存在的话，先创建这个目录
//        dir.mkdirs();
//
//        Set<File> swapFiles = new HashSet<>();
//        File[] files = dir.listFiles();
//
//        // todo swap 文件的校验
//        //
//        //        for (File file : files) {
//        //            if (file.isFile()) {
//        //
//        //                if (!file.canRead()) {
//        //                    throw new HanabiException("Could not read file " + file);
//        //                }
//        //
//        //                String fileName = file.getName();
//        //                if (fileName.endsWith(LogConstant.SwapFileSuffix)) {
//        //
//        //                }
//        //            }
//        //        }
//
//        for (File file : files) {
//            if (file.isFile()) {
//                String fileName = file.getName();
//
//                // 确保索引文件有相应的操作日志文件
//                if (fileName.endsWith(LogCommon.IndexFileSuffix)) {
//                    File logFile = new File(file.getAbsolutePath()
//                                                .replace(LogCommon.IndexFileSuffix, LogCommon.LogFileSuffix));
//                    if (!logFile.exists()) {
//                        file.delete();
//                        logger.error("索引文件 {} 没有对应的日志文件，已将其删除。", fileName);
//                    }
//                } else if (fileName.endsWith(LogCommon.LogFileSuffix)) {
//                    File indexFile = new File(file.getAbsolutePath()
//                                                  .replace(LogCommon.LogFileSuffix, LogCommon.IndexFileSuffix));
//
//                    long startOffset = Long.valueOf(fileName.substring(0, fileName.length() - LogCommon.LogFileSuffix.length()));
//                    LogSegment logSegment = new LogSegment(dir, startOffset, LogConfigHelper.getIndexInterval(), LogConfigHelper.getIndexInterval());
//
//                    if (indexFile.exists()) {
//                        try {
//                            logSegment.getOffsetIndex()
//                                      .sanityCheck();
//                        } catch (OffsetIndexIllegalException e) {
//                            logger.error(e.getMessage());
//                            logger.error("正在重建 {} 的索引文件", fileName);
//                            indexFile.delete();
//                            logSegment.recover(LogConfigHelper.getMaxLogMessageSize());
//                        }
//                    } else {
//                        logSegment.recover(LogConfigHelper.getMaxLogMessageSize());
//                    }
//
//                    segments.put(startOffset, logSegment);
//                }
//            }
//        }
//    }
//
//    private LogSegment maybeRoll(int msgSize) {
//        LogSegment logSegment = activeSegment();
//        if (logSegment.size() > LogConfigHelper.getMaxLogSegmentSize() - msgSize ||
//            logSegment.getOffsetIndex()
//                      .isFull()) {
//            logger.info("即将开启新的日志分片，上个分片大小为 {}/{}， 对应的索引文件共建立了 {}/{} 个索引。", logSegment.size(), LogConfigHelper.getMaxLogSegmentSize(),
//                logSegment.getOffsetIndex()
//                          .getEntries(), logSegment.getOffsetIndex()
//                                                   .getMaxEntries());
//
//            return roll();
//        } else {
//            return logSegment;
//        }
//    }
//
//    /**
//     * Roll the log over to a new active segment starting with the current logEndOffset.
//     * This will trim the index to the exact size of the number of entries it currently contains.
//     *
//     * @return The newly rolled segment
//     */
//    private LogSegment roll() {
//        return this.lockSupplier(() -> {
//            // todo 还要考虑换代的问题
//            long newOffset = currentOffset + 1;
//            File newFile = LogCommon.logFilename(dir, newOffset);
//            File indexFile = LogCommon.indexFilename(dir, newOffset);
//
//            Lists.newArrayList(newFile, indexFile)
//                 .forEach(file -> Optional.of(newFile)
//                                          .filter(File::exists)
//                                          .ifPresent(f -> {
//                                              logger.info("新创建的日志分片或索引竟然已经存在，将其抹除。");
//                                              f.delete();
//                                          }));
//
//            Optional.ofNullable(segments.lastEntry())
//                    .ifPresent(e -> {
//                        e.getValue()
//                         .getOffsetIndex()
//                         .trimToValidSize();
//                        e.getValue()
//                         .getFileOperationSet()
//                         .trim();
//                    });
//
//            LogSegment newLogSegment;
//            try {
//                newLogSegment = new LogSegment(dir, newOffset, LogConfigHelper.getIndexInterval(), LogConfigHelper.getMaxIndexSize());
//            } catch (IOException e) {
//                logger.error("滚动时创建新的日志分片失败，分片目录：{}, 创建的文件为：{}", dir.getAbsolutePath(), newFile.getName());
//                throw new HanabiException("滚动时创建新的日志分片失败");
//            }
//
//            if (addSegment(newLogSegment) != null) {
//                logger.error("滚动时创建新的日志分片失败，该分片已经存在");
//            }
//
//            // We need to update the segment base offset and append position data of the metadata when log rolls.
//            // The next offset should not change.
//            updateLogEndOffset(nextOffsetMetadata.getMessageOffset());
//
//            HanabiExecutors.submit(() -> flush(newOffset));
//
//            return newLogSegment;
//        });
//    }
//
//    public void updateLogEndOffset(long offset) {
//
//    }
//
//    /**
//     * The number of messages appended to the log since the last flush
//     */
//    public long unflushedMessages() {
//        return logEndOffset - recoveryPoint;
//    }
//
//    /**
//     * Flush all log segments
//     */
//    private void flush() {
//        flush(logEndOffset);
//    }
//
//    /**
//     * Flush log segments for all offsets up to offset -1
//     */
//    private void flush(long offset) {
//        if (offset <= recoveryPoint) {
//            return;
//        }
//
//        logger.debug("将日志 {} 刷盘，现刷盘至 offset {}，上次刷盘时间为 {}，现共有 {} 条消息还未刷盘。", name(), offset, lastflushedTime.get(), unflushedMessages());
//
//        for (LogSegment logSegment : getLogSegments(recoveryPoint, offset)) {
//            logSegment.flush();
//        }
//
//        lockSupplier(() -> {
//            if (offset > this.recoveryPoint) {
//                this.recoveryPoint = offset;
//                lastflushedTime.set(System.currentTimeMillis());
//            }
//            return null;
//        });
//    }
//
//    /**
//     * Calculate the offset of the next message
//     */
//    private LogSegment activeSegment() {
//        return segments.lastEntry()
//                       .getValue();
//    }
//
//    /**
//     * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
//     *
//     * @param segment The segment to add
//     */
//    public LogSegment addSegment(LogSegment segment) {
//        return this.segments.put(segment.getBaseOffset(), segment);
//    }
//
//    public String name() {
//        return dir.getName();
//    }
//
//    /**
//     * Get all segments beginning with the segment that includes "from" and ending with the segment
//     * that includes up to "to-1" or the end of the log (if to > logEndOffset)
//     */
//    public Iterable<LogSegment> getLogSegments(long fromOffset, long toOffset) {
//        return lockSupplier(() -> {
//            Long floor = segments.floorKey(fromOffset);
//            if (floor == null) {
//                return segments.headMap(toOffset)
//                               .values();
//            } else {
//                return segments.subMap(floor, true, toOffset, false)
//                               .values();
//            }
//        });
//    }
//}
