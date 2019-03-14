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

package com.anur.core.store.operationset;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import com.anur.core.store.common.OffsetAndPosition;
import com.anur.core.store.common.Operation;
import com.anur.core.store.common.OperationAndOffset;
import com.anur.core.store.common.OperationConstant;
import com.anur.core.util.FileIOUtil;
import com.anur.core.util.IteratorTemplate;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 *
 * 高仿kafka FileMessageSet 写的，是操作日志在磁盘的映射类。
 */
public class FileOperationSet extends OperationSet {

    /**
     * 用于读写日志，是此文件的 channel
     */
    private final FileChannel fileChannel;

    /**
     * OperationSet 开始的绝对位置的下界
     */
    private final int start;

    /**
     * OperationSet 结束的绝对位置的上限
     */
    private final int end;

    /**
     * 本FileOperationSet的大小
     */
    private final AtomicInteger _size;

    /**
     * 对应着磁盘上的一个文件
     */
    private volatile File file;

    /**
     * 表示这个类是否表示文件的一部分，也就是切片
     */
    private boolean isSlice;

    /**
     * 基础构造函数 => 创建一个 FileOperationSet
     */
    private FileOperationSet(File file, FileChannel fileChannel, int start, int end, boolean isSlice) throws IOException {
        this.file = file;
        this.fileChannel = fileChannel;
        this.start = start;
        this.end = end;

        AtomicInteger s;
        if (isSlice) {
            s = new AtomicInteger(end - start);
        } else {
            int channelEnd = Math.min((int) fileChannel.size(), end);
            s = new AtomicInteger(channelEnd - start);
            fileChannel.position(channelEnd);
        }
        this._size = s;
    }

    /**
     * 创建一个非分片的FileOperationSet
     */
    public FileOperationSet(File file, FileChannel fileChannel) throws IOException {
        this(file, fileChannel, 0, Integer.MAX_VALUE, false);
    }

    /**
     * 创建一个非分片的FileOperationSet
     */
    public FileOperationSet(File file) throws IOException {
        this(file, FileIOUtil.openChannel(file, true));
    }

    /**
     * Create a file message set with mutable option
     */
    public FileOperationSet(File file, boolean mutable) throws IOException {
        this(file, FileIOUtil.openChannel(file, mutable));
    }

    /**
     * Create a slice view of the file message set that begins and ends at the given byte offsets
     */
    public FileOperationSet(File file, FileChannel channel, int start, int end) throws IOException {
        this(file, channel, start, end, true);
    }

    /**
     * 将操作记录添加到文件中
     */
    public void append(ByteBufferOperationSet byteBufferOperationSet) throws IOException {
        int written = byteBufferOperationSet.writeFullyTo(this.fileChannel);
        this._size.getAndAdd(written);
    }

    /**
     * Return a message set which is a view into this set starting from the given position and with the given size limit.
     *
     * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
     *
     * If this message set is already sliced, the position will be taken relative to that slicing.
     *
     * 返回当前FileMessageSet中的一部分FileMessageSet，不会真正将内容读到内存中
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     *
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    public FileOperationSet read(int position, int size) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("Invalid position: " + position);
        }
        if (size < 0) {
            throw new IllegalArgumentException("Invalid size: " + size);
        }
        return new FileOperationSet(file,
            fileChannel,
            this.start + position,
            Math.min(this.start + position + size, sizeInBytes()));
    }

    /**
     * 从startingPosition开始，找到第一个大于等于目标offset的物理地址
     *
     * 如果找不到，则返回null
     */
    public OffsetAndPosition searchFor(long targetOffset, int startingPosition) throws IOException {
        int position = startingPosition;
        ByteBuffer buffer = ByteBuffer.allocate(OperationSet.LogOverhead);
        int size = this.sizeInBytes();

        // 没有越界之前，可以无限搜索
        while (position + OperationSet.LogOverhead < size) {
            buffer.rewind(); // 重置一下buffer指针
            fileChannel.read(buffer, position); // 读取文件的offset值

            if (buffer.hasRemaining()) {
                throw new IllegalStateException(String.format("Failed to read complete buffer for targetOffset %d startPosition %d in %s", targetOffset, startingPosition, file.getAbsolutePath()));
            }
            buffer.rewind(); // 重置一下buffer指针

            long offset = buffer.getLong();
            if (offset >= targetOffset) {
                return new OffsetAndPosition(offset, position);
            }

            int messageSize = buffer.getInt(); // 8字节的offset后面紧跟着4字节的这条消息的长度

            if (messageSize < OperationConstant.MinMessageOverhead) {
                throw new IllegalStateException("Invalid message size: " + messageSize);
            }

            position += OperationSet.LogOverhead + messageSize;
        }
        return null;
    }

    /**
     * 将某个日志文件进行裁剪到指定大小，必须小于文件的size
     */
    public int truncateTo(int targetSize) {
        int originalSize = this.sizeInBytes();
        if (targetSize > originalSize || targetSize < 0) {
            throw new HanabiException("尝试将日志文件截短成 " + targetSize + " bytes 但是失败了, " +
                " 原文件大小为 " + originalSize + " bytes。");
        }

        try {
            if (targetSize < fileChannel.size()) {
                fileChannel.truncate(targetSize);
                fileChannel.position(targetSize);
                _size.set(targetSize);
            }
        } catch (IOException e) {
            throw new HanabiException("尝试将日志文件截短成 " + targetSize + " bytes 但是失败了, " +
                " 原文件大小为 " + originalSize + " bytes。");
        }
        return originalSize - targetSize;
    }

    /**
     * The number of bytes taken up by this file set
     */
    public int sizeInBytes() {
        return _size.get();
    }

    /**
     * Write some of this set to the given channel.
     *
     * 将FileMessageSet的部分数据写到指定的channel上
     *
     * @param destChannel The channel to write to.
     * @param writePosition The position in the message set to begin writing from.
     * @param size The maximum number of bytes to write
     *
     * @return The number of bytes actually written.
     */
    @Override
    public int writeTo(GatheringByteChannel destChannel, long writePosition, int size) throws IOException {
        // 进行边界检查
        int newSize = Math.min((int) fileChannel.size(), end) - start;
        if (newSize < _size.get()) {
            throw new HanabiException(String.format("FileOperationSet 的文件大小 %s 在写的过程中被截断了：之前的文件大小为 %d, 现在的文件大小为 %d", file.getAbsolutePath(), _size.get(), newSize));
        }
        int position = start + (int) writePosition; // The position in the message set to begin writing from.
        int count = Math.min(size, sizeInBytes());

        // 将从position开始的count个bytes写到指定到channel中
        return (int) fileChannel.transferTo(position, count, destChannel);
    }

    /**
     * 获取各种消息的迭代器
     */
    @Override
    public Iterator<OperationAndOffset> iterator() {
        return this.iterator(Integer.MAX_VALUE);
    }

    /**
     * 获取某个文件的迭代器
     */
    public Iterator<OperationAndOffset> iterator(int maxMessageSize) {
        return new IteratorTemplate<OperationAndOffset>() {

            private int location = start;

            private ByteBuffer sizeOffsetBuffer = ByteBuffer.allocate(OperationSet.LogOverhead);

            @Override
            protected OperationAndOffset makeNext() {
                if (location + OperationSet.LogOverhead >= end) {// 如果已经到了末尾，返回空
                    return allDone();
                }

                // read the size of the item
                sizeOffsetBuffer.rewind();
                try {
                    fileChannel.read(sizeOffsetBuffer, location);
                } catch (IOException e) {
                    throw new HanabiException("Error occurred while reading data from fileChannel.");
                }
                if (sizeOffsetBuffer.hasRemaining()) {// 这也是读到了末尾
                    return allDone();
                }

                sizeOffsetBuffer.flip();
                long offset = sizeOffsetBuffer.getLong();
                int size = sizeOffsetBuffer.getInt();

                if (size < OperationConstant.MinMessageOverhead || location + OperationSet.LogOverhead + size > end) { // 代表消息放不下了
                    return allDone();
                }

                if (size > maxMessageSize) {
                    throw new HanabiException(String.format("Message size exceeds the largest allowable message size (%d).", maxMessageSize));
                }

                // read the item itself
                ByteBuffer buffer = ByteBuffer.allocate(size);
                try {
                    fileChannel.read(buffer, location + OperationSet.LogOverhead);
                } catch (IOException e) {
                    throw new HanabiException("Error occurred while reading data from fileChannel.");
                }
                if (buffer.hasRemaining()) {// 代表没读完，其实和上面一样，可能是消息写到最后写不下了，或者被截取截没了
                    return allDone();
                }
                buffer.rewind();

                // increment the location and return the item
                location += size + OperationSet.LogOverhead;
                return new OperationAndOffset(new Operation(buffer), offset);
            }
        };
    }

    public void trim() {
        truncateTo(sizeInBytes());
    }

    public void flush() {
        try {
            fileChannel.force(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
