package com.anur.core.protocol.operation;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
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
     * 向指定的channel从某个指定的offset开始，将OperationSet写入这个channel，
     * 这个方法将返回已经写入的字节
     */
    abstract int writeTo(GatheringByteChannel channel, long offset, int maxSize);

    /**
     * 返回此OperationSet内部的迭代器
     */
    abstract Iterator<OperationAndOffset> iterator();
}
