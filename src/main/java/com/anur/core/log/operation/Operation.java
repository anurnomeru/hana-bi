package com.anur.core.log.operation;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import com.anur.core.log.common.OperationConstant;
import com.anur.core.log.common.OperationTypeEnum;
import com.anur.core.util.ByteBufferUtil;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 *
 * 记录了某一个操作
 */
public class Operation {

    private ByteBuffer buffer;

    private OperationTypeEnum operationTypeEnum;

    private String key;

    private String value;

    private Operation(OperationTypeEnum operationTypeEnum, String key, String value) {
        this.operationTypeEnum = operationTypeEnum;
        this.key = key;
        this.value = value;

        int operationType = operationTypeEnum.byteSign;
        byte[] kBytes = key.getBytes(Charset.defaultCharset());
        int kSize = kBytes.length;
        byte[] vBytes = value.getBytes(Charset.defaultCharset());
        int vSize = vBytes.length;

        ByteBuffer byteBuffer = ByteBuffer.allocate(OperationConstant.BaseMessageOverhead + kSize + vSize);

        byteBuffer.position(OperationConstant.TypeOffset);
        byteBuffer.putInt(operationType);
        byteBuffer.putInt(kSize);
        byteBuffer.put(kBytes);
        byteBuffer.putInt(vSize);
        byteBuffer.put(vBytes);
        this.buffer = byteBuffer;

        long crc = computeChecksum();

        byteBuffer.position(0);
        byteBuffer.putInt((int) crc);

        byteBuffer.rewind();
    }

    public Operation(ByteBuffer buffer) {
        buffer.mark();

        this.buffer = buffer;

        buffer.position(OperationConstant.TypeOffset);
        this.operationTypeEnum = OperationTypeEnum.parseBybyteSign(buffer.getInt());

        int kSize = buffer.getInt();
        byte[] kByte = new byte[kSize];
        buffer.get(kByte);
        this.key = new String(kByte);

        int vSize = buffer.getInt();
        byte[] vByte = new byte[vSize];
        buffer.get(vByte);
        this.value = new String(vByte);

        ensureValid();
        buffer.reset();
    }

    public ByteBuffer getByteBuffer() {
        return buffer;
    }

    public int size() {
        return buffer.limit();
    }

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

    public OperationTypeEnum getOperationTypeEnum() {
        return operationTypeEnum;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
