package com.anur.core.struct.base;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import com.anur.core.struct.OperationTypeEnum;

/**
 * Created by Anur IjuoKaruKas on 2019/4/2
 *
 * 带有时间戳的消息，用于协调器之间通讯用
 */
public abstract class AbstractTimedStruct extends AbstractStruct {

    public static final int TimestampOffset = TypeOffset + TypeLength;

    public static final int TimestampLength = 8;

    public static final int OriginMessageOverhead = TimestampOffset + TimestampLength;

    public void init(ByteBuffer byteBuffer, OperationTypeEnum operationTypeEnum) {
        buffer = byteBuffer;
        byteBuffer.position(TypeOffset);
        byteBuffer.putInt(operationTypeEnum.byteSign);
        byteBuffer.putLong(System.currentTimeMillis());
    }

    public void init(int capacity, OperationTypeEnum operationTypeEnum, Consumer<ByteBuffer> then) {
        buffer = ByteBuffer.allocate(capacity);
        buffer.position(TypeOffset);
        buffer.putInt(operationTypeEnum.byteSign);
        buffer.putLong(System.currentTimeMillis());
        then.accept(buffer);
        buffer.flip();
    }

    /**
     * 时间戳用于防止同一次请求的 “多次请求”，保证幂等性
     */
    public long getTimeMillis() {
        return buffer.getLong(TimestampOffset);
    }
}
