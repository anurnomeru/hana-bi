package com.anur.core.util;

import java.nio.ByteBuffer;
import com.anur.core.log.operation.ByteBufferOperationSet;
import com.anur.core.log.operation.Operation;

/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 *
 * 一些和 Operation 相关的 ByteBuffer 操作
 */
public class ByteBufferUtil {

    private static final ByteBufferOperationSet Empty = new ByteBufferOperationSet(ByteBuffer.allocate(0));
    //
    //    /**
    //     * 传入一系列的operation，并为其分配序列
    //     */
    //    private static ByteBuffer create(OffsetAssigner offsetAssigner, Collection<Operation> operationCollection) {
    //        if (operationCollection == null || operationCollection.isEmpty()) {
    //            return Empty.getByteBuffer();
    //        } else {
    //
    //            // 这个操作集合需要预留的空间大小
    //            ByteBuffer byteBuffer = ByteBuffer.allocate(OperationSet.messageSetSize(operationCollection));
    //            for (Operation operation : operationCollection) {
    //                OperationByteBufferUtil.writeMessage(byteBuffer, operation, offsetAssigner.nextAbsoluteOffset());
    //            }
    //            byteBuffer.rewind();
    //            return byteBuffer;
    //        }
    //    }

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

    /**
     * Compute the CRC32 of the byte array
     *
     * @param bytes The array to compute the checksum for
     *
     * @return The CRC32
     */
    public static long crc32(byte[] bytes) {
        return crc32(bytes, 0, bytes.length);
    }

    /**
     * @param offset 从哪一位开始计算offset
     * @param size 从上面开始，计算xx位
     */
    public static long crc32(byte[] bytes, int offset, int size) {
        Crc32 crc = new Crc32();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }
}
