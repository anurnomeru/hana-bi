package com.anur.io.store.common;

/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 *
 * 和基础 operation 相关的常数
 */
public class OperationConstant {

    /**
     * 一个Operation由以下部分组成：
     *
     * 　4　   +   4    +    4      + key +    4        +  v
     * CRC32  +  type  + keyLength + key + valueLength +  v
     */
    public static final int CrcOffset = 0;

    public static final int CrcLength = 4;

    public static final int TypeOffset = CrcOffset + CrcLength;

    public static final int TypeLength = 4;

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
}
