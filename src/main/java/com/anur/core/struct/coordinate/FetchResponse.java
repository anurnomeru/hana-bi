package com.anur.core.struct.coordinate;

import java.nio.ByteBuffer;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.AbstractTimedStruct;
import com.anur.io.store.common.FetchDataInfo;
import com.anur.io.store.operationset.ByteBufferOperationSet;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 2019/3/28
 *
 * 获取 AOF 用
 *
 * 由 4位CRC + 4位类型 + 8位时间戳 + 4位内容长度 + 内容 组成
 *
 * 子类可以实现其 content 部分的内容拓展
 */
public class FetchResponse extends AbstractTimedStruct {

    public static final int ByteBufferOperationSetSizeOffset = TimestampOffset + TimestampLength;

    public static final int ByteBufferOperationSetLength = 4;

    /**
     * 最基础的 FetchResponse 大小 ( 不包括byteBufferOperationSet )
     */
    public static final int BaseMessageOverhead = ByteBufferOperationSetSizeOffset + ByteBufferOperationSetLength;

    private ByteBufferOperationSet byteBufferOperationSet;

    public FetchResponse(FetchDataInfo fetchDataInfo) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BaseMessageOverhead);
        init(byteBuffer, OperationTypeEnum.FETCH_RESPONSE);

        fetchDataInfo.getOperationSet()
        byteBuffer.putInt();

        byteBuffer.rewind();
    }

    public FetchResponse(ByteBuffer byteBuffer) {
        buffer = byteBuffer;
    }

    @Override
    public void writeIntoChannel(Channel channel) {
        channel.write(buffer);
    }

    @Override
    public int totalSize() {
        return 0;
    }
}
