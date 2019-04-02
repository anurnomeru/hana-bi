package com.anur.core.struct.base;

import java.nio.ByteBuffer;
import com.anur.core.struct.OperationTypeEnum;

/**
 * Created by Anur IjuoKaruKas on 2019/4/2
 *
 * 带有时间戳的消息，用于协调器之间通讯用
 */
public abstract class AbstractTimedStruct extends AbstractStruct {

    public static final int TimestampOffset = TypeOffset + TypeLength;

    public static final int TimestampLength = 8;

    public void init(ByteBuffer byteBuffer, OperationTypeEnum operationTypeEnum) {
        byteBuffer.position(TypeOffset);
        byteBuffer.putInt(operationTypeEnum.byteSign);
        byteBuffer.putLong(System.currentTimeMillis());
    }

    /**
     * 时间戳用于防止同一次请求的 “多次请求”，保证幂等性
     */
    public long getTimeMillis() {
        return buffer.getLong(TimestampOffset);
    }
}
