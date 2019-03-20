package com.anur.io.store.log;

import java.nio.ByteBuffer;
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
public class PreLog extends ReentrantLocker {

    private static final int maxByteBufOperationSize = 10;

    private final long generation;

    private ConcurrentSkipListMap<Long, CompositeByteBuf> preLog;

    private int byteBufOperationSize = 0;

    public PreLog(long generation) {
        this.preLog = new ConcurrentSkipListMap<>();
        this.generation = generation;
    }

    public static void main(String[] args) {
        PreLog preLog = new PreLog(10);

        for (int i = 0; i < 1000; i++) {
            preLog.append(new Operation(OperationTypeEnum.REGISTER, "", ""), i);
        }

        preLog.getBefore(990);
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
            byteBuf.addComponent(Unpooled.wrappedBuffer(operation.getByteBuffer()));

            return null;
        });
    }

    /**
     * 获取此消息以及之前的消息
     */
    public ByteBuf getBefore(long offset) {
        ConcurrentNavigableMap<Long, CompositeByteBuf> result = preLog.headMap(offset, true);

        if (result.size() == 0) {
            return null;
        }

        CompositeByteBuf lastOne = result.lastEntry()
                                         .getValue();

        CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
        for (Entry<Long, CompositeByteBuf> e : result.entrySet()) {
            if (e.getValue() != lastOne) {
                compositeByteBuf.addComponent(e.getValue());
            } else {
                // 因为ByteBuffer里面可以装 maxByteBufOperationSize 个 byteBuffer，所以...
                ByteBuf dup = lastOne.duplicate();
                int toPosition = 0;

                ByteBuf offsetAndSize = Unpooled.buffer(12);
                dup.readBytes(offsetAndSize);

                offsetAndSize.readLong();
            }
        }

        return null;
    }
}
