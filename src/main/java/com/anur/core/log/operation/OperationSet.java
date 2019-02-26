package com.anur.core.log.operation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 *
 * ”仿照“Kafka写的日志存储类
 */
public abstract class OperationSet {

    public static final int MessageSizeLength = 4;

    public static final int OffsetLength = 8;

    public static final int LogOverhead = MessageSizeLength + OffsetLength;

    public static final ByteBufferOperationSet Empty = new ByteBufferOperationSet(ByteBuffer.allocate(0));

    /**
     * The size of a message set containing the given operationCollections
     *
     * 循环 operationCollection，得出里面所有 Operation 在装载完 LogOverhead 后的大小
     * 注意，这里在每个 Operation 的前面还预留了 LogOverhead 的大小
     */
    public static int messageSetSize(Collection<Operation> operationCollection) {
        return operationCollection.stream()
                                  .map(OperationSet::entrySize)
                                  .reduce((i1, i2) -> i1 + i2)
                                  .orElse(0);
    }

    /**
     * The size of a size-delimited entry in a operationSet
     *
     */
    public static int entrySize(Operation operation) {
        return LogOverhead + operation.size();
    }

    /**
     * 向指定的channel从某个指定的offset开始，将OperationSet写入这个channel，
     * 这个方法将返回已经写入的字节
     */
    public abstract int writeTo(GatheringByteChannel channel, long offset, int maxSize) throws IOException;

    /**
     * 返回此OperationSet内部的迭代器
     */
    public abstract Iterator<OperationAndOffset> iterator();
}
