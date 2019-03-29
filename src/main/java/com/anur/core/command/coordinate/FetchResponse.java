package com.anur.core.command.coordinate;

import java.nio.ByteBuffer;
import com.anur.core.command.common.OperationTypeEnum;
import com.anur.core.command.core.AbstractCommand;
import com.anur.io.store.operationset.ByteBufferOperationSet;
import io.netty.buffer.ByteBuf;

/**
 * Created by Anur IjuoKaruKas on 2019/3/28
 *
 * 获取 AOF 用
 *
 * 由 4位CRC + 4位类型 + 8位时间戳 + 4位内容长度 + 内容 组成
 *
 * 子类可以实现其 content 部分的内容拓展
 */
public class FetchResponse extends AbstractCommand {

    public static final int TimestampOffset = TypeOffset + TypeLength;

    public static final int TimestampLength = 8;

    public static final int ContentSizeOffset = TimestampOffset + TimestampLength;

    public static final int ContentSizeLength = 4;

    /**
     * 最基础的 FetchResponse 大小 ( 不包括byteBufferOperationSet )
     */
    public static final int BaseMessageOverhead = ContentSizeOffset + ContentSizeLength;

    // =================================================================

    private long timestamp;

    private int contentLength;

    private ByteBufferOperationSet byteBufferOperationSet;

    public FetchResponse(OperationTypeEnum operationTypeEnum, long timestamp, int contentLength, ByteBufferOperationSet byteBufferOperationSet) {
        this.timestamp = timestamp;
        this.contentLength = contentLength;

        int crc = 0; // FetchResponse 不需要 CRC
        int operationType = operationTypeEnum.byteSign;
        int size = byteBufferOperationSet.sizeInBytes();

//        ByteBuf byteBuf = ByteBuffer.allocate(BaseMessageOverhead);
    }
}
