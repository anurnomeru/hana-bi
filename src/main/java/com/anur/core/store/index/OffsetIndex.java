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
package com.anur.core.store.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.lock.ReentrantLocker;
import com.anur.core.store.common.OffsetAndPosition;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 *
 * 每个磁盘上 OperationSet 的索引文件，仿照 Kafka OffsetIndex 所写
 */
public class OffsetIndex extends ReentrantLocker {

    private static Logger logger = LoggerFactory.getLogger(OffsetIndex.class);

    /**
     * factor为什么是8，因为index是由 4 位的相对 offset + 4位的 position 组成
     */
    private static final int FACTOR = 8;

    private volatile File file;

    private final long baseOffset;

    private final int maxIndexSize;

    private volatile MappedByteBuffer mmap;

    /* the number of eight-byte entries currently in the index */
    private volatile int entries;// 索引个数

    /* The maximum number of eight-byte entries this index can hold */
    private volatile int maxEntries;// 最大索引个数

    private volatile long lastOffset;// 最后一个索引的 offset

    /**
     * 基础构造函数 => 创建一个日志文件分片索引
     */
    public OffsetIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;

        // 判断索引文件是否已经被创建
        boolean newlyCreated = this.file.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        try {
            if (newlyCreated) {
                if (maxIndexSize < FACTOR) {
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize);
                }
                // 对于新创建的文件来说，需要进行扩容，扩容为小于 maxIndexSize 最大的8的倍数
                raf.setLength(roundToExactMultiple(maxIndexSize, FACTOR));
            }

            // 映射从 0 - len 的这段内容
            long len = raf.length();
            this.mmap = raf.getChannel()
                           .map(FileChannel.MapMode.READ_WRITE, 0, len);

            if (newlyCreated) {// 新创建的索引文件从0开始写
                this.mmap.position(0);
            } else {// 非新创建的则从limit，也就是读的尽头开始写
                this.mmap.position(roundToExactMultiple(this.mmap.limit(), FACTOR));
            }
        } finally {
            raf.close();
        }

        this.entries = this.mmap.position() / FACTOR;
        this.maxEntries = this.mmap.limit() / FACTOR;
        this.lastOffset = readLastEntry().getOffset();
    }

    /**
     * The last entry in the index
     */
    private OffsetAndPosition readLastEntry() {
        return this.lockSupplier(() -> {
            switch (entries) {
            case 0:
                return new OffsetAndPosition(baseOffset, 0);
            default:
                return new OffsetAndPosition(baseOffset + relativeOffset(mmap, entries - 1), physical(mmap, entries - 1));
            }
        });
    }

    /**
     * Find the largest offset less than or equal to the given targetOffset
     * and return a pair holding this offset and its corresponding physical file position.
     *
     * 找寻小于或者等于传入 offset 的最大 offset 索引，返回这个索引的绝对 offset 和 position
     *
     * @param targetOffset The offset to look up.
     *
     * @return The offset found and the corresponding file position for this offset.
     * If the target offset is smaller than the least entry in the index (or the index is empty),
     * the pair (baseOffset, 0) is returned.
     */
    public OffsetAndPosition lookup(long targetOffset) {
        return this.lockSupplier(() -> {
            ByteBuffer idx = mmap.duplicate();// 创建一个副本
            int slot = indexSlotFor(idx, targetOffset);
            if (slot == -1) {
                return new OffsetAndPosition(baseOffset, 0);
            } else {
                // 返回slot这个索引的绝对 offset 和 position
                return new OffsetAndPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot));
            }
        });
    }

    /**
     * Find the slot in which the largest offset less than or equal to the given
     * target offset is stored.
     *
     * @param idx The index buffer
     * @param targetOffset The offset to look for
     *
     * @return The slot found or -1 if the least entry in the index is larger than the target offset or the index is empty
     */
    private int indexSlotFor(ByteBuffer idx, long targetOffset) {
        // we only store the difference from the base offset so calculate that
        // 将目标的 offset 转为相对 offset
        long relOffset = targetOffset - baseOffset;

        // check if the index is empty
        if (entries == 0) {
            return -1;
        }

        // check if the target offset is smaller than the least offset
        if (relativeOffset(idx, 0) > relOffset) {
            return -1;
        }

        // binary search for the entry
        int lo = 0;
        int hi = entries - 1;
        while (lo < hi) {// 二分法寻找，如果实在找不到，返回小的那一个，比如索引位置只有 100, 200 ,300, 查找 150 则返回100
            // mid 为索引个数的中间值
            int mid = (int) Math.ceil(hi / 2.0 + lo / 2.0);

            // 寻找这个中间值的相对 offset
            int found = relativeOffset(idx, mid);
            if (found == relOffset) {
                return mid;
            } else if (found < relOffset) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     * E.g. roundToExactMultiple(67, 8) == 64
     *
     * 抹除某个值的余数
     */
    private int roundToExactMultiple(int number, int factor) {
        return factor * (number / factor);
    }

    /* return the nth offset relative to the base offset */
    private int relativeOffset(ByteBuffer buffer, int n) {
        return buffer.getInt(n * FACTOR);// 返回相对 offset
    }

    /* return the nth physical position */
    private int physical(ByteBuffer buffer, int n) {
        return buffer.getInt(n * FACTOR + 4);// 返回 position
    }

    /**
     * Get the nth offset mapping from the index
     *
     * @param n The entry number in the index
     *
     * @return The offset/position pair at that entry
     */
    public OffsetAndPosition entry(int n) {
        return this.lockSupplier(() -> {
            if (n >= entries) {
                throw new IllegalArgumentException(String.format("Attempt to fetch the %dth entry from an index of size %d.", n, entries));
            }

            ByteBuffer idx = mmap.duplicate();
            return new OffsetAndPosition(relativeOffset(idx, n), physical(idx, n));
        });
    }

    /**
     * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
     *
     * 追加 offset + position 到索引之中，offset 必须大于其他索引的offset
     */
    public void append(long offset, int position) {
        this.lockSupplier(() -> {
            if (isFull()) {
                throw new HanabiException("Attempt to append to a full index (size = " + entries + ").");
            }
            if (entries == 0 || offset > lastOffset) {
                logger.info("在索引文件 {} 为 offset 为 {} 的日志添加 position 为 {} 的索引", baseOffset + ".index", offset, position);
                mmap.putInt((int) (offset - baseOffset));
                mmap.putInt(position);
                entries++;
                lastOffset = offset;

                if (entries * 8 != mmap.position()) {
                    throw new HanabiException(
                        entries + " entries but file position in index is " + mmap.position() + ".");
                }
            } else {
                throw new HanabiException(
                    String.format("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s.", offset, entries, lastOffset, file.getAbsolutePath()));
            }
            return null;
        });
    }

    /**
     * True iff there are no more slots available in this index
     */
    public boolean isFull() {
        return entries >= maxEntries;
    }

    /**
     * Truncate the entire index, deleting all entries
     */
    public void truncate() {
        truncateToEntries(0);
    }

    /**
     * 移除所有大于等于传入 offset 的索引，如果传入的 offset 比最大的索引还大，则返回空
     *
     * Remove all entries from the index which have an offset greater than or equal to the given offset.
     * Truncating to an offset larger than the largest in the index has no effect.
     */
    public void truncateTo(long offset) {
        lockSupplier(() -> {
            ByteBuffer idx = mmap.duplicate();
            int slot = indexSlotFor(idx, offset);

            /* There are 3 cases for choosing the new size
             * 1) if there is no entry in the index <= the offset, delete everything
             * 2) if there is an entry for this exact offset, delete it and everything larger than it
             * 3) if there is no entry for this offset, delete everything larger than the next smallest
             */
            int newEntries;
            if (slot < 0) {
                newEntries = 0;
            } else if (relativeOffset(idx, slot) == offset - baseOffset) {
                newEntries = slot;
            } else {
                newEntries = slot + 1;
            }

            truncateToEntries(newEntries);
            return null;
        });
    }

    /**
     * Truncates index to a known number of entries.
     */
    private void truncateToEntries(int entries) {
        lockSupplier(() -> {
            this.entries = entries;
            mmap.position(entries * 8);
            lastOffset = readLastEntry().getOffset();
            return null;
        });
    }

    /**
     * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
     * the file.
     */
    public void trimToValidSize() {
        lockSupplier(() -> {
            resize(entries * 8);
            return null;
        });
    }

    /**
     * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
     * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
     * loading segments from disk or truncating back to an old segment where a new log segment became active;
     * we want to reset the index size to maximum index size to avoid rolling new segment.
     */
    public void resize(int newSize) {
        lockSupplier(() -> {
            RandomAccessFile raf = null;

            try {
                raf = new RandomAccessFile(file, "rw");
            } catch (FileNotFoundException e) {
                throw new HanabiException(e);
            }

            int roundedNewSize = roundToExactMultiple(newSize, 8);
            int position = mmap.position();

            /* Windows won't let us modify the file length while the file is mmapped :-( */
            if (mmap.limit() != 0) {// TODO 不知道为什么为 0 的时候会报错
                forceUnmap(mmap);
            }
            try {
                raf.setLength(roundedNewSize);
                mmap = raf.getChannel()
                          .map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize);
                maxEntries = mmap.limit() / 8;
                mmap.position(position);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    raf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        });
    }

    /**
     * Forcefully free the buffer's mmap. We do this only on windows.
     */
    private void forceUnmap(MappedByteBuffer m) {
        try {
            if (m instanceof sun.nio.ch.DirectBuffer) {
                ((sun.nio.ch.DirectBuffer) m).cleaner()
                                             .clean();
            }
        } catch (Throwable e) {
            throw new HanabiException("Error when freeing index buffer");
        }
    }

    /**
     * Flush the data in the index to disk
     */
    public void flush() {
        lockSupplier(() -> {
            mmap.force();
            return null;
        });
    }

    /**
     * Delete this index file
     */
    public boolean delete() {
        forceUnmap(mmap);
        return file.delete();
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public int getEntries() {
        return entries;
    }

    public int getMaxIndexSize() {
        return maxIndexSize;
    }

    /**
     * 简单的核验一下这个索引文件有无大问题
     */
    public void sanityCheck() {
        if (!(entries == 0 || lastOffset > baseOffset)) {
            throw new OffsetIndexIllegalException(String.format("Corrupt index found, index file (%s) has non-zero size but the last offset is %s and the base offset is %s",
                file.getAbsolutePath(), lastOffset, baseOffset));
        }
        if (file.length() % 8 != 0) {
            throw new OffsetIndexIllegalException("Index file " + file.getName() + " is corrupt, found " + file.length() +
                " bytes which is not positive or not a multiple of 8.");
        }
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public static class OffsetIndexIllegalException extends HanabiException {

        public OffsetIndexIllegalException(String message) {
            super(message);
        }
    }
}
