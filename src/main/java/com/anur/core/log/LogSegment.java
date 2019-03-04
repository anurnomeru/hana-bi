/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.anur.core.log;

import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.log.common.OffsetAndPosition;
import com.anur.core.log.index.OffsetIndex;
import com.anur.core.log.operation.ByteBufferOperationSet;
import com.anur.core.log.operation.FileOperationSet;
import com.anur.core.log.operation.OperationSet;

/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 *
 * 仿照自 Kafka LogSegment
 */
public class LogSegment {

    private Logger logger = LoggerFactory.getLogger(LogSegment.class);

    /**
     * 管理的那个日志截片
     */
    private final FileOperationSet fileOperationSet;

    /**
     * 日志截片的索引文件
     */
    private final OffsetIndex offsetIndex;

    /**
     * 该日志文件从哪个offset开始
     */
    private final long baseOffset;

    /**
     * 索引字节间隔
     */
    private final int indexIntervalBytes;

    /**
     * 距离上一次添加索引，已经写了多少个字节了
     */
    private int bytesSinceLastIndexEntry = 0;

    /**
     * 基础构造函数 => 创建一个日志文件分片
     */
    private LogSegment(FileOperationSet fileOperationSet, OffsetIndex offsetIndex, long baseOffset, int indexIntervalBytes) {
        this.fileOperationSet = fileOperationSet;
        this.offsetIndex = offsetIndex;
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
    }

    public LogSegment(File dir, long startOffset, int indexIntervalBytes, int maxIndexSize) throws IOException {
        this(
            new FileOperationSet(LogCommon.logFilename(dir, startOffset)),
            new OffsetIndex(LogCommon.logFilename(dir, startOffset),
                startOffset, maxIndexSize),
            startOffset,
            indexIntervalBytes);
    }

    /**
     * 将传入的 ByteBufferOperationSet 追加到文件之中，offset的值为 messages 的初始offset
     */
    public void append(long offset, ByteBufferOperationSet messages) throws IOException {
        if (messages.sizeInBytes() > 0) {
            logger.debug("Inserting {} bytes at offset {} at position {}", messages.sizeInBytes(), offset, fileOperationSet.sizeInBytes());
            // append an entry to the index (if needed)
            // 追加到了一定的容量，添加索引
            if (bytesSinceLastIndexEntry > indexIntervalBytes) {

                // 追加 offset 以及当前文件的 size，也就是写的 position到索引文件中
                offsetIndex.append(offset, fileOperationSet.sizeInBytes());
                this.bytesSinceLastIndexEntry = 0;
            }
            // 追加消息到 fileOperationSet 中
            // append the messages
            fileOperationSet.append(messages);
            this.bytesSinceLastIndexEntry += messages.sizeInBytes();
        }
    }

    /**
     * 找遍整个文件，找到第一个大于等于目标 offset 的地址信息
     */
    public OffsetAndPosition translateOffset(long offset) throws IOException {
        return translateOffset(offset, 0);
    }

    /**
     * 从 startingPosition开始，找到第一个大于等于目标 offset 的地址信息
     */
    public OffsetAndPosition translateOffset(long offset, int startingPosition) throws IOException {

        // 找寻小于或者等于传入 offset 的最大 offset 索引，返回这个索引的绝对 offset 和 position
        OffsetAndPosition offsetAndPosition = offsetIndex.lookup(offset);

        // 从 startingPosition开始 ，找到第一个大于等于目标offset的物理地址
        return fileOperationSet.searchFor(offset, Math.max(offsetAndPosition.getPosition(), startingPosition));
    }

    public FetchDataInfo read(long startOffset, Long maxOffset, int maxSize) throws IOException {
        return read(startOffset, maxOffset, maxSize, fileOperationSet.sizeInBytes());
    }

    /**
     * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
     * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
     *
     * 从这个日志文件中读取一个 message set，读取从 startOffset 开始，如果指定了 maxOffset， 这个 message set 将不会包含大于 maxSize 的数据，
     * 并且在 maxOffset 之前结束。
     *
     * @param startOffset A lower bound on the first offset to include in the message set we read
     * @param maxSize The maximum number of bytes to include in the message set we read
     * @param maxOffset An optional maximum offset for the message set we read
     * @param maxPosition The maximum position in the log segment that should be exposed for read
     *
     * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
     * or null if the startOffset is larger than the largest offset in this log
     *
     * 返回获取到的数据以及 第一个 offset 相关的元数据，这个 offset >= startOffset。
     * 如果 startOffset 大于这个日志文件存储的最大的 offset ，将返回 null
     */
    public FetchDataInfo read(long startOffset, Long maxOffset, int maxSize, long maxPosition) throws IOException {
        if (maxSize < 0) {
            throw new IllegalArgumentException(String.format("Invalid max size for log read (%d)", maxSize));
        }

        int logSize = fileOperationSet.sizeInBytes(); // this may change, need to save a consistent copy
        OffsetAndPosition startPosition = translateOffset(startOffset);// 查找第一个大于等于 startOffset 的 Offset 和 Position

        if (startPosition == null) {
            return null;// 代表 fileOperationSet 里最大的 offset 也没startOffset大
        }

        LogOffsetMetadata logOffsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition.getPosition());

        // if the size is zero, still return a log segment but with zero size
        if (maxSize == 0) {
            return new FetchDataInfo(logOffsetMetadata, OperationSet.Empty);
        }

        int length = 0;

        if (maxOffset == null) {
            // length 取 maxPosition - 第一个大于等于 startOffset 的 Position，最大不超过 maxSize
            length = (int) Math.min(maxPosition - startPosition.getPosition(), maxSize);
        } else {
            if (maxOffset < startOffset) {
                return new FetchDataInfo(logOffsetMetadata, OperationSet.Empty);
            }
            // 查找第一个大于等于 maxOffset 的 Offset 和 Position
            OffsetAndPosition end = translateOffset(maxOffset, startPosition.getPosition());
            int endPosition;
            if (end == null) {
                endPosition = logSize;// end最大只能取到logSize
            } else {
                endPosition = end.getPosition();
            }

            length = (int) Math.min(
                Math.min(maxPosition, endPosition) - startPosition.getPosition(),
                maxSize
            );
        }

        return new FetchDataInfo(logOffsetMetadata, fileOperationSet.read(startPosition.getPosition(), length));
    }

    /**
     * 传入 offset 如若有效，则
     * 1、移除大于等于此 offset 的所有索引
     * 2、移除大于等于此 offset 的所有操作日志
     */
    public int truncateTo(long offset) throws IOException {
        OffsetAndPosition offsetAndPosition = translateOffset(offset);
        if (offsetAndPosition == null) {
            return 0;
        }

        offsetIndex.truncateTo(offset);

        // after truncation, reset and allocate more space for the (new currently  active) index
        offsetIndex.resize(offsetIndex.getMaxIndexSize());

        int bytesTruncated = fileOperationSet.truncateTo(offsetAndPosition.getPosition());

        //        if(log.sizeInBytes == 0){
        //            // kafka存在删除一整个日志文件的情况
        //        }

        bytesSinceLastIndexEntry = 0;
        return bytesTruncated;
    }

    public long size() {
        return fileOperationSet.sizeInBytes();
    }

    public long nextOffset() {
        // 原kafka实现方式：通过 read(index.lastOffset, None, log.sizeInBytes) 来获取第一个大于等于 log.sizeInBytes 的信息，实际上就是下一条信息
        // TODO 使用统一的ID生成工具
        return 0;
    }

    public long getBaseOffset() {
        return baseOffset;
    }
}
