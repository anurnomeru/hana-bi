package com.anur.core.log.core;

/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 *
 * 和基础 operation 相关的常熟
 */
public class OperationConstant {

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

    /** 一定要有key */
    private static final int minKeyLength = 1;

    private static final int valueSizeLength = 4;

    /** 一定要有value */
    private static final int minValueLength = 1;

    /**
     * 最小的Operation长度为这个，小于这个不可能构成一条消息，最起码要满足
     *
     * CRC32 + generation + serial +  type  + (keySize = 1) + key + (valueSize = 1) + value
     */
    public static final int MinMessageOverhead = keyOffset + valueSizeLength + minKeyLength + minValueLength;
}
