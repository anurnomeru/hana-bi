package com.anur.core.log.index;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import com.anur.core.lock.ReentrantLocker;
import com.anur.core.log.common.OffsetAndPosition;
import com.sun.java.util.jar.pack.Instruction.Switch;

/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 *
 * 每个磁盘上 OperationSet 的索引文件
 */
public class OffsetIndex extends ReentrantLocker {

    private static final int FACTOR = 8;

    private File file;

    private long baseOffset;

    private int maxIndexSize;

    private volatile MappedByteBuffer mmap;

    /* the number of eight-byte entries currently in the index */
    private volatile int entries;

    /* The maximum number of eight-byte entries this index can hold */
    private volatile int maxEntries;

    private volatile long lastOffset;

    public OffsetIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;

        boolean newlyCreated = this.file.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        try {

            if (newlyCreated) {
                if (maxIndexSize < FACTOR) {
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize);
                }
                raf.setLength(roundToExactMultiple(maxIndexSize, FACTOR));
            }

            // 映射从 0 - len 的这段内容
            long len = raf.length();
            this.mmap = raf.getChannel()
                           .map(FileChannel.MapMode.READ_WRITE, 0, len);

            if (newlyCreated) {
                this.mmap.position(0);
            } else {
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
     * @param targetOffset The offset to look up.
     *
     * @return The offset found and the corresponding file position for this offset.
     * If the target offset is smaller than the least entry in the index (or the index is empty),
     * the pair (baseOffset, 0) is returned.
     */
    private OffsetAndPosition lookup(long targetOffset) {
        return this.lockSupplier(() -> {
            ByteBuffer idx = mmap.duplicate();
            int slot = indexSlotFor(idx, targetOffset);
            if (slot == -1) {
                return new OffsetAndPosition(baseOffset, 0);
            } else {
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
        while (lo < hi) {
            int mid = (int) Math.ceil(hi / 2.0 + lo / 2.0);
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
        return buffer.getInt(n * FACTOR);
    }

    /* return the nth physical position */
    private int physical(ByteBuffer buffer, int n) {
        return buffer.getInt(n * FACTOR + 4);
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
}
