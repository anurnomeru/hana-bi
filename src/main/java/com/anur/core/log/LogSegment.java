package com.anur.core.log;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.log.common.OffsetAndPosition;
import com.anur.core.log.index.OffsetIndex;
import com.anur.core.log.operation.ByteBufferOperationSet;
import com.anur.core.log.operation.FileOperationSet;

/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 */
public class LogSegment {

    private Logger logger = LoggerFactory.getLogger(LogSegment.class);

    /**
     * 管理的那个日志截片
     */
    private FileOperationSet fileOperationSet;

    /**
     * 日志截片的索引文件
     */
    private OffsetIndex offsetIndex;

    /**
     * 该日志文件从哪个offset开始
     */
    private long baseOffset;

    /**
     * 索引字节间隔
     */
    private int indexIntervalBytes;

    //    /**
    //     *
    //     */
    //    private long rollJitterMs;

    /**
     * 距离上一次添加索引，已经写了多少个字节了
     */
    private int bytesSinceLastIndexEntry = 0;

    public void append(long offset, ByteBufferOperationSet messages) throws IOException {
        if (messages.sizeInBytes() > 0) {
            logger.debug("Inserting {} bytes at offset {} at position {}", messages.sizeInBytes(), offset, fileOperationSet.sizeInBytes());
            // append an entry to the index (if needed)
            // 追加到了一定的容量，添加索引
            if (bytesSinceLastIndexEntry > indexIntervalBytes) {
                offsetIndex.append(offset, fileOperationSet.sizeInBytes());
                this.bytesSinceLastIndexEntry = 0;
            }
            // 追加消息到 fileOperationSet 中
            // append the messages
            fileOperationSet.append(messages);
            this.bytesSinceLastIndexEntry += messages.sizeInBytes();
        }
    }

    /**
     * 通过绝对 Offset 查找大于等于 startingPosition 的第一个 Offset 和 Position
     */
    public OffsetAndPosition translateOffset(long offset, int startingPosition) throws IOException {

        // 找寻小于或者等于传入 offset 的最大 offset 索引，返回这个索引的绝对 offset 和 position
        OffsetAndPosition offsetAndPosition = offsetIndex.lookup(offset);

        // 从 startingPosition开始 ，找到第一个大于等于目标offset的物理地址
        return fileOperationSet.searchFor(offset, Math.max(offsetAndPosition.getPosition(), startingPosition));
    }
}
