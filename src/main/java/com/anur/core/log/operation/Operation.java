package com.anur.core.log.operation;

import java.nio.ByteBuffer;
import com.anur.core.log.common.OperationConstant;
import com.anur.core.util.ByteBufferUtil;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 *
 * 记录了某一个操作
 */
public class Operation {

    private ByteBuffer buffer;

    public Operation(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public ByteBuffer getByteBuffer() {
        return buffer;
    }

    /**
     * The complete serialized size of this operation in bytes (including crc, header attributes, etc)
     */
    public int size() {
        return buffer.limit();
    }

    /**
     * Throw an InvalidMessageException if isValid is false for this message
     */
    public void ensureValid() {
        long stored = checkSum();
        long compute = computeChecksum();
        if (stored != compute) {
            throw new HanabiException(String.format("Message is corrupt (stored crc = %s, computed crc = %s)", stored, compute));
        }
    }

    public long checkSum() {
        return ByteBufferUtil.readUnsignedInt(buffer, OperationConstant.CrcOffset);
    }

    public long computeChecksum() {
        return ByteBufferUtil.crc32(buffer.array(), buffer.arrayOffset() + OperationConstant.TypeOffset, buffer.limit() - OperationConstant.TypeOffset);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
}
