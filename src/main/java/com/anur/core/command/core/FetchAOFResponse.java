package com.anur.core.command.core;

import java.nio.ByteBuffer;
import com.anur.core.command.common.OperationTypeEnum;
import com.anur.io.store.operationset.ByteBufferOperationSet;

/**
 * Created by Anur IjuoKaruKas on 2019/3/28
 *
 * 获取 AOF 用
 *
 * 由 4位CRC + 4位类型 + 8位时间戳 + 4位内容长度 + 内容 组成
 *
 * 子类可以实现其 content 部分的内容拓展
 */
public class FetchAOFResponse {

    private ByteBuffer content;

    private OperationTypeEnum operationTypeEnum;

    private long timestamp;

    private int contentLength;

    private ByteBufferOperationSet byteBufferOperationSet;

    public FetchAOFResponse(OperationTypeEnum operationTypeEnum, long timestamp, int contentLength, ByteBuffer content) {
        this.operationTypeEnum = operationTypeEnum;
        this.timestamp = timestamp;
        this.contentLength = contentLength;
        this.content = content;
    }
}
