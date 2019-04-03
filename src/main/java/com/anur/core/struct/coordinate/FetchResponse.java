package com.anur.core.struct.coordinate;

import java.nio.ByteBuffer;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.AbstractTimedStruct;
import com.anur.io.store.common.FetchDataInfo;
import com.anur.io.store.operationset.ByteBufferOperationSet;
import com.anur.io.store.operationset.FileOperationSet;
import io.netty.channel.Channel;
import io.netty.channel.DefaultFileRegion;

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

    public static final int FileOperationSetSizeOffset = TimestampOffset + TimestampLength;

    public static final int FileOperationSetSizeLength = 4;

    public static final int GenerationOffset = FileOperationSetSizeOffset + FileOperationSetSizeLength;

    public static final int GenerationLength = 8;

    /**
     * 最基础的 FetchResponse 大小 ( 不包括byteBufferOperationSet )
     */
    public static final int BaseMessageOverhead = GenerationOffset + GenerationLength;

    private FileOperationSet fileOperationSet;

    public FetchResponse(FetchDataInfo fetchDataInfo) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BaseMessageOverhead);
        init(byteBuffer, OperationTypeEnum.FETCH_RESPONSE);

        fileOperationSet = fetchDataInfo.getFileOperationSet();
        byteBuffer.putInt(fileOperationSet.sizeInBytes());

        byteBuffer.rewind();
    }

    public FetchResponse(ByteBuffer byteBuffer) {
        buffer = byteBuffer;
    }

    public long getGeneration() {
        return buffer.getLong(GenerationOffset);
    }

    public ByteBufferOperationSet read() {
        buffer.position(BaseMessageOverhead);
        return new ByteBufferOperationSet(buffer.slice());
    }

    @Override
    public void writeIntoChannel(Channel channel) {
        channel.write(buffer);
        channel.write(new DefaultFileRegion(fileOperationSet.getFileChannel(), fileOperationSet.getStart(), fileOperationSet.getEnd()));
    }

    @Override
    public int totalSize() {
        return size() + fileOperationSet.sizeInBytes();
    }
}
