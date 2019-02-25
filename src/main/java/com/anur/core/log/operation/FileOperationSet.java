package com.anur.core.log.operation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 */
public class FileOperationSet extends OperationSet {

    /**
     * 对应着磁盘上的一个文件
     */
    private volatile File file;

    /**
     * 用于读写日志，是此文件的 channel
     */
    private FileChannel fileChannel;

    /**
     * OperationSet 开始的绝对位置的下界
     */
    private int start;

    /**
     * OperationSet 结束的绝对位置的上限
     */
    private int end;

    /**
     * 表示这个类是否表示文件的一部分，也就是切片
     */
    private boolean isSlice;

    /**
     * 本FileOperationSet的大小
     */
    private AtomicInteger _size;

    /**
     * 创建一个非分片的FileOperationSet
     */
    public FileOperationSet(File file) throws IOException {
        this.file = file;
        this.fileChannel = FileOperationSet.openChannel(file, true);
        this.start = 0;
        this.end = Integer.MAX_VALUE;
        this.isSlice = false;
        this._size = new AtomicInteger(Math.min((int) fileChannel.size(), end));
    }

    /**
     * 创建一个非分片的FileOperationSet
     */
    public FileOperationSet(File file, boolean mutable) throws IOException {
        this.file = file;
        this.fileChannel = FileOperationSet.openChannel(file, mutable);
        this.start = 0;
        this.end = Integer.MAX_VALUE;
        this.isSlice = false;
        this._size = new AtomicInteger(Math.min((int) fileChannel.size(), end));
    }

    /**
     * 创建一个非分片的FileOperationSet
     */
    public FileOperationSet(File file, FileChannel fileChannel) throws IOException {
        this.file = file;
        this.fileChannel = fileChannel;
        this.start = 0;
        this.end = Integer.MAX_VALUE;
        this.isSlice = false;
        this._size = new AtomicInteger(Math.min((int) fileChannel.size(), end));
    }

    /**
     * 创建一个文件分片
     */
    public FileOperationSet(File file, FileChannel fileChannel, int start, int end) throws IOException {
        this.file = file;
        this.fileChannel = fileChannel;
        this.start = start;
        this.end = end;
        this.isSlice = true;
        this._size = new AtomicInteger(end - start);// 如果只是一个切片，我们不用去检查它的大小
    }

    /**
     * 将操作记录添加到文件中
     */
    public void append(ByteBufferOperationSet byteBufferOperationSet) throws IOException {
        int written = byteBufferOperationSet.writeFullyTo(this.fileChannel);
        this._size.getAndAdd(written);
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

        }
    }

    /**
     * The number of bytes taken up by this file set
     */
    public int sizeInBytes() {
        return _size.get();
    }

    /**
     * 开启一个文件channel
     *
     * mutable，是否可改变（是否不可读）
     * true 可读可写
     * false 只可读
     */
    public static FileChannel openChannel(File file, boolean mutable) throws FileNotFoundException {
        if (mutable) {
            return new RandomAccessFile(file, "rw").getChannel();
        } else {
            return new FileInputStream(file).getChannel();
        }
    }

    @Override
    int writeTo(GatheringByteChannel channel, long offset, int maxSize) {
        return 0;
    }

    @Override
    Iterator<OperationAndOffset> iterator() {
        return null;
    }
}
