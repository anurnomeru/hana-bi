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

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.log.common.OffsetAndPosition;
import com.anur.core.log.index.OffsetIndex;
import com.anur.core.log.operation.ByteBufferOperationSet;
import com.anur.core.log.operation.FileOperationSet;

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
    private FileOperationSet fileOperationSet;

    /**
     * 日志截片的索引文件
     */
    private OffsetIndex offsetIndex;

    /**
     * 该日志文件从哪个offset开始
     */
    private long baseOffset;

    /**
     * 索引字节间隔
     */
    private int indexIntervalBytes;

    /**
     * 距离上一次添加索引，已经写了多少个字节了
     */
    private int bytesSinceLastIndexEntry = 0;

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
     * 通过绝对 Offset 查找大于等于 0 的第一个 Offset 和 Position
     */
    public OffsetAndPosition translateOffset(long offset) throws IOException {
        return translateOffset(offset, 0);
    }

    /**
     * 通过绝对 Offset 查找第一个大于等于 startingPosition 的 Offset 和 Position
     */
    public OffsetAndPosition translateOffset(long offset, int startingPosition) throws IOException {

        // 找寻小于或者等于传入 offset 的最大 offset 索引，返回这个索引的绝对 offset 和 position
        OffsetAndPosition offsetAndPosition = offsetIndex.lookup(offset);

        // 从 startingPosition开始 ，找到第一个大于等于目标offset的物理地址
        return fileOperationSet.searchFor(offset, Math.max(offsetAndPosition.getPosition(), startingPosition));
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
    public FetchDataInfo read(long startOffset, long maxOffset, int maxSize, long maxPosition) throws IOException {
        if (maxSize < 0) {
            throw new IllegalArgumentException(String.format("Invalid max size for log read (%d)", maxSize));
        }

        int logSize = fileOperationSet.sizeInBytes(); // this may change, need to save a consistent copy
        OffsetAndPosition startPosition = translateOffset(startOffset);//查找第一个大于等于 startPosition 的 Offset 和 Position


        if (startPosition == null) {
            return null;// 代表 fileOperationSet 里最大的 offset 也没startOffset大
        }
    }
}
