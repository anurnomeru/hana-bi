package com.anur.io.store.prelog;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import com.anur.core.lock.ReentrantLocker;
import com.anur.io.store.common.Operation;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Created by Anur IjuoKaruKas on 2019/3/18
 *
 * 内存版预日志，主要用于子节点不断从leader节点同步预消息
 */
public class ByteBufPreLog extends ReentrantLocker {

    private static final int maxByteBufOperationSize = 10;

    private final long generation;

    private ConcurrentSkipListMap<Long, CompositeByteBuf> preLog;

    private int byteBufOperationSize = 0;

    public ByteBufPreLog(long generation) {
        this.preLog = new ConcurrentSkipListMap<>();
        this.generation = generation;
    }

    /**
     * 将消息添加到内存中
     */
    public void append(Operation operation, long offset) {
        this.lockSupplier(() -> {
            byteBufOperationSize++;
            CompositeByteBuf byteBuf;

            if (preLog.size() != 0 && byteBufOperationSize < maxByteBufOperationSize) {
                byteBuf = preLog.lastEntry()
                                .getValue();
            } else {
                byteBufOperationSize = 0;
                byteBuf = Unpooled.compositeBuffer();
                preLog.put(offset, byteBuf);
            }

            int size = operation.size();
            byteBuf.writeLong(offset);
            byteBuf.writeInt(size);
            byteBuf.addComponent(true, Unpooled.wrappedBuffer(operation.getByteBuffer()));
            return null;
        });
    }

    /**
     * 获取此消息之后的消息（不包括 targetOffset 这一条）
     */
    public ByteBuf getAfter(long targetOffset) {
        ConcurrentNavigableMap<Long, CompositeByteBuf> result = preLog.tailMap(targetOffset, true);

        if (result.size() == 0) {
            result = preLog.tailMap(targetOffset - maxByteBufOperationSize, true);
        }

        if (result.size() == 0) {
            return null;
        }

        CompositeByteBuf firstOne = result.firstEntry()
                                          .getValue();

        CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
        for (Entry<Long, CompositeByteBuf> e : result.entrySet()) {
            if (e.getValue() != firstOne) {
                compositeByteBuf.addComponent(true, e.getValue());
            } else {
                // 因为ByteBuffer里面可以装 maxByteBufOperationSize 个 byteBuffer，所以...
                ByteBuf dup = firstOne.duplicate();
                ByteBuf offsetAndSize = Unpooled.buffer(12);

                long offset = -1;
                int size = -1;

                while (offset != targetOffset) {
                    offsetAndSize.discardReadBytes();

                    if (dup.readableBytes() < 12) {
                        break;
                    }

                    dup.readBytes(offsetAndSize);

                    offset = offsetAndSize.readLong();
                    size = offsetAndSize.readInt();

                    dup.readerIndex(dup.readerIndex() + size);
                }

                compositeByteBuf.addComponent(true, dup.slice());
            }
        }

        return compositeByteBuf.writerIndex() == 0 ? null : compositeByteBuf;
    }

    /**
     * 获取此消息之前的消息（不包括 targetOffset 这一条）
     */
    public ByteBuf getBefore(long targetOffset) {
        ConcurrentNavigableMap<Long, CompositeByteBuf> result = preLog.headMap(targetOffset, true);

        if (result.size() == 0) {
            result = preLog.tailMap(targetOffset - maxByteBufOperationSize, true);
        }

        if (result.size() == 0) {
            return null;
        }

        CompositeByteBuf lastOne = result.lastEntry()
                                         .getValue();

        CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
        for (Entry<Long, CompositeByteBuf> e : result.entrySet()) {
            if (e.getValue() != lastOne) {
                compositeByteBuf.addComponent(true, e.getValue());
            } else {
                // 因为ByteBuffer里面可以装 maxByteBufOperationSize 个 byteBuffer，所以...
                ByteBuf dup = lastOne.duplicate();
                ByteBuf offsetAndSize = Unpooled.buffer(12);

                long offset = -1;
                int size = -1;

                while (offset != targetOffset) {
                    offsetAndSize.discardReadBytes();

                    if (dup.readableBytes() < 12) {
                        break;
                    }

                    dup.readBytes(offsetAndSize);

                    offset = offsetAndSize.readLong();
                    size = offsetAndSize.readInt();

                    dup.readerIndex(dup.readerIndex() + size);
                }

                compositeByteBuf.addComponent(true, dup.slice(0, dup.readerIndex()));
            }
        }

        return compositeByteBuf.writerIndex() == 0 ? null : compositeByteBuf;
    }

    public void discardBefore(long offset) {
        ConcurrentNavigableMap<Long, CompositeByteBuf> discardMap = preLog.subMap(0L, true, offset - maxByteBufOperationSize, false);
        for (Long key : discardMap.keySet()) {
            preLog.remove(key);
        }
    }
}
