package com.anur.core.command.core;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import com.anur.core.command.common.OperationTypeEnum;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 *
 * Operation 是对应最基本的操作，这些操作将被写入日志
 *
 * 一个Operation由以下部分组成：
 *
 * 　4　   +   4    +    4      + key +    4        +  v
 * CRC32  +  type  + keyLength + key + valueLength +  v
 */
public class Operation extends AbstractCommand {

    public static final int KeySizeOffset = TypeOffset + TypeLength;

    public static final int KeySizeLength = 4;

    public static final int KeyOffset = KeySizeOffset + KeySizeLength;

    /** 一定要有key */
    public static final int MinKeyLength = 1;

    public static final int ValueSizeLength = 4;

    /**
     * 除去消息头，最小的Operation长度为这个，小于这个不可能构成一条消息，最起码要满足
     *
     * CRC32 +  type  + (KeySize = 1) + key + (ValueSize = 1)
     */
    public static final int MinMessageOverhead = KeyOffset + ValueSizeLength + MinKeyLength;

    /**
     * 最基础的operation大小
     */
    public static final int BaseMessageOverhead = KeyOffset + ValueSizeLength;

    // =================================================================

    private String key;

    private String value;

    public Operation(OperationTypeEnum operationTypeEnum, String key, String value) {
        this.key = key;
        this.value = value;

        int operationType = operationTypeEnum.byteSign;
        byte[] kBytes = key.getBytes(Charset.defaultCharset());
        int kSize = kBytes.length;
        byte[] vBytes = value.getBytes(Charset.defaultCharset());
        int vSize = vBytes.length;

        ByteBuffer byteBuffer = ByteBuffer.allocate(BaseMessageOverhead + kSize + vSize);

        byteBuffer.position(TypeOffset);
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

        buffer.position(KeySizeOffset);

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

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Operation{" +
            "operationTypeEnum=" + getOperationTypeEnum() +
            ", key='" + key + '\'' +
            ", value='" + value + '\'' +
            '}';
    }
}
