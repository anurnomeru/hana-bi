package com.anur.core.log.operation;

import java.nio.ByteBuffer;
/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 *
 * 记录了某一个操作
 */
public class Operation {

    private ByteBuffer byteBuffer;

    public Operation(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    /**
     * The complete serialized size of this operation in bytes (including crc, header attributes, etc)
     */
    public int size() {
        return byteBuffer.limit();
    }
}
