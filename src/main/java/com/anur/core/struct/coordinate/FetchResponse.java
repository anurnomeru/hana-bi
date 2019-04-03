package com.anur.core.struct.coordinate;

import java.nio.ByteBuffer;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.AbstractTimedStruct;
import com.anur.io.store.common.FetchDataInfo;
import com.anur.io.store.operationset.ByteBufferOperationSet;
import com.anur.io.store.operationset.FileOperationSet;
import io.netty.buffer.Unpooled;
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

    public static final int GenerationOffset = TimestampOffset + TimestampLength;

    public static final int GenerationLength = 8;

    public static final int FileOperationSetSizeOffset = GenerationOffset + GenerationLength;

    public static final int FileOperationSetSizeLength = 4;

    /**
     * 最基础的 FetchResponse 大小 ( 不包括byteBufferOperationSet )
     */
    public static final int BaseMessageOverhead = FileOperationSetSizeOffset + FileOperationSetSizeLength;

    private final int fileOperationSetSize;

    private FileOperationSet fileOperationSet;

    public FetchResponse(FetchDataInfo fetchDataInfo) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BaseMessageOverhead);
        init(byteBuffer, OperationTypeEnum.FETCH_RESPONSE);

        // 为空代表已无更多更新的消息
        if (fetchDataInfo == null) {
            fileOperationSetSize = 0;
            byteBuffer.putLong(-1);
            byteBuffer.putInt(0);
        } else {
            fileOperationSet = fetchDataInfo.getFileOperationSet();
            fileOperationSetSize = fileOperationSet.sizeInBytes();
            byteBuffer.putLong(fetchDataInfo.getFetchOffsetMetadata()
                                            .getGeneration());
            byteBuffer.putInt(fileOperationSetSize);
        }

        byteBuffer.rewind();
        byteBuffer.limit(BaseMessageOverhead);
    }

    public FetchResponse(ByteBuffer byteBuffer) {
        buffer = byteBuffer;
        fileOperationSetSize = buffer.getInt(BaseMessageOverhead);
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
        channel.write(Unpooled.wrappedBuffer(buffer));
        if (fileOperationSetSize > 0) {
            channel.write(new DefaultFileRegion(fileOperationSet.getFileChannel(), fileOperationSet.getStart(), fileOperationSet.getEnd()));
        }
    }

    @Override
    public int totalSize() {
        return size() + fileOperationSetSize;
    }
}
