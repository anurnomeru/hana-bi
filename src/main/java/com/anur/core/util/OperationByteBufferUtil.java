package com.anur.core.util;

import java.nio.ByteBuffer;
import java.util.Collection;
import com.anur.core.log.operation.ByteBufferOperationSet;
import com.anur.core.log.operation.ByteBufferOperationSet.OffsetAssigner;
import com.anur.core.log.operation.Operation;
import com.anur.core.log.operation.OperationSet;

/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 *
 * 一些和 Operation 相关的 ByteBuffer 操作
 */
public class OperationByteBufferUtil {

    private static final ByteBufferOperationSet Empty = new ByteBufferOperationSet(ByteBuffer.allocate(0));

    /**
     * 传入一系列的operation，并为其分配序列
     */
    private static ByteBuffer create(OffsetAssigner offsetAssigner, Collection<Operation> operationCollection) {
        if (operationCollection == null || operationCollection.isEmpty()) {
            return Empty.getByteBuffer();
        } else {

            // 这个操作集合需要预留的空间大小
            ByteBuffer byteBuffer = ByteBuffer.allocate(OperationSet.messageSetSize(operationCollection));
            for (Operation operation : operationCollection) {
                OperationByteBufferUtil.writeMessage(byteBuffer, operation, offsetAssigner.nextAbsoluteOffset());
            }
            byteBuffer.rewind();
            return byteBuffer;
        }
    }

    /**
     * 将一个 operation，写入 buffer 中，并为其分配 offset
     */
    public static void writeMessage(ByteBuffer buffer, Operation operation, long offset) {
        buffer.putLong(offset);
        buffer.putInt(operation.size());
        buffer.put(operation.getByteBuffer());
        operation.getByteBuffer()
                 .rewind();
    }
}
