package com.anur.core.log.operation;

import java.nio.ByteBuffer;
/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 *
 * 记录了某一个操作
 */
public class Operation {

    /**
     * 一个Operation由以下部分组成：
     *
     * 　4　  +     8      +   8    +   4    +    4      + key +    4        +  v
     * CRC32 + generation + serial +  type  + keyLength + key + valueLength +  v
     */
    private static final int CrcOffset = 0;

    private static final int CrcLength = 4;

    private static final int GenerationOffset = CrcOffset + CrcLength;

    private static final int GenerationLength = 8;

    private static final int serialOffset = GenerationOffset + GenerationLength;

    private static final int serialLength = 8;

    private static final int typeOffset = serialOffset + serialLength;

    private static final int typeLength = 4;

    private static final int keySizeOffset = typeOffset + typeLength;

    private static final int keySizeLength = 4;

    private static final int keyOffset = keySizeOffset + keySizeLength;

    private static final int valueSizeLength = 4;

    /**
     * 最小的Operation长度为这个，小于这个不可能构成一条消息，最起码要满足
     *
     * CRC32 + generation + serial +  type  + (keyLength = 0) + (valueLength = 0)
     */
    private static final int MinMessageOverhead = keyOffset + valueSizeLength;

    public static int getMinMessageOverhead() {
        return MinMessageOverhead;
    }

    private ByteBuffer byteBuffer;

    public Operation(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }
}
