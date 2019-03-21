package com.anur.io.store.prelog;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import com.anur.core.lock.ReentrantLocker;
import com.anur.io.store.common.Operation;
import com.anur.io.store.common.OperationTypeEnum;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Created by Anur IjuoKaruKas on 2019/3/18
 *
 * 预日志
 */
public class ByteBufPreLog extends ReentrantLocker implements PreLogger {

    private static final int maxByteBufOperationSize = 10;

    private final long generation;

    private ConcurrentSkipListMap<Long, CompositeByteBuf> preLog;

    private int byteBufOperationSize = 0;

    public ByteBufPreLog(long generation) {
        this.preLog = new ConcurrentSkipListMap<>();
        this.generation = generation;
    }

    public static void main(String[] args) {
        ByteBufPreLog preLog = new ByteBufPreLog(10);

        for (int i = 0; i < 1000; i++) {
            preLog.append(new Operation(OperationTypeEnum.REGISTER, "", ""), i);
        }

        ByteBuf byteBuf = preLog.getAfter(995, true);
    }

    /**
     * 将消息添加到内存中
     */
    @Override
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
            byteBuf.addComponent(Unpooled.wrappedBuffer(operation.getByteBuffer()));

            return null;
        });
    }

    /**
     * 获取此消息之后的消息
     */
    @Override
    public ByteBuf getAfter(long targetOffset, boolean inclusive) {
        ConcurrentNavigableMap<Long, CompositeByteBuf> result = preLog.tailMap(targetOffset, inclusive);

        if (result.size() == 0) {
            result = preLog.tailMap(targetOffset - maxByteBufOperationSize, inclusive);
        }

        CompositeByteBuf firstOne = result.firstEntry()
                                          .getValue();

        CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
        for (Entry<Long, CompositeByteBuf> e : result.entrySet()) {
            if (e.getValue() != firstOne) {
                compositeByteBuf.addComponent(e.getValue());
            } else {
                // 因为ByteBuffer里面可以装 maxByteBufOperationSize 个 byteBuffer，所以...
                ByteBuf dup = firstOne.duplicate();
                ByteBuf offsetAndSize = Unpooled.buffer(12);

                long offset = -1;
                int size = -1;

                while (offset != targetOffset) {
                    offsetAndSize.discardReadBytes();

                    dup.readBytes(offsetAndSize);

                    offset = offsetAndSize.readLong();
                    size = offsetAndSize.readInt();

                    dup.readerIndex(dup.readerIndex() + size);
                }

                compositeByteBuf.addComponent(dup.slice());
            }
        }

        return null;
    }
}
