package com.anur.core.command.core;

import java.nio.ByteBuffer;
import com.anur.core.command.common.OperationTypeEnum;

/**
 * Created by Anur IjuoKaruKas on 2019/3/28
 *
 * 不同于 {@link Operation} 这个是一个通用的指令范式
 * 由 4位CRC + 4位类型 + 8位时间戳 + 4位内容长度 + 内容 组成
 *
 * 子类可以实现其 content 部分的内容拓展
 */
public class Command {

    private ByteBuffer content;

    private OperationTypeEnum operationTypeEnum;

    private long timestamp;

    private int contentLength;

    public Command(OperationTypeEnum operationTypeEnum, long timestamp, int contentLength, ByteBuffer content) {
        this.operationTypeEnum = operationTypeEnum;
        this.timestamp = timestamp;
        this.contentLength = contentLength;
        this.content = content;
    }
}
