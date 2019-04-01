package com.anur.io.store.prelog;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import com.anur.core.lock.ReentrantLocker;
import com.anur.core.command.modle.Operation;
import com.anur.io.store.common.OperationAndOffset;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Created by Anur IjuoKaruKas on 2019/3/18
 *
 * 内存版预日志，主要用于子节点不断从leader节点同步预消息
 */
public class ByteBufPreLog extends ReentrantLocker {

    private final long generation;

    private ConcurrentNavigableMap<Long, OperationAndOffset> preLog;

    public ByteBufPreLog(long generation) {
        this.preLog = new ConcurrentSkipListMap<>();
        this.generation = generation;
    }

    /**
     * 将消息添加到内存中
     */
    public void append(Operation operation, long offset) {
        preLog.put(offset, new OperationAndOffset(operation, offset));
    }

    /**
     * 获取此消息之前的消息（包括 targetOffset 这一条）
     */
    public PreLogMeta getBefore(long targetOffset) {
        ConcurrentNavigableMap<Long, OperationAndOffset> result = preLog.tailMap(targetOffset, true);
        return result.size() == 0 ? null : new PreLogMeta(result.firstKey(), result.lastKey(), result.values());
    }

    /**
     * 丢弃之前的消息们
     */
    public void discardBefore(long targetOffset) {
        ConcurrentNavigableMap<Long, OperationAndOffset> discardMap = preLog.tailMap(targetOffset, false);
        for (Long key : discardMap.keySet()) {
            preLog.remove(key);
        }
    }

    public static class PreLogMeta {

        public final long startOffset;

        public final long endOffset;

        public final Collection<OperationAndOffset> offsets;

        public PreLogMeta(long startOffset, long endOffset, Collection<OperationAndOffset> offsets) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.offsets = offsets;
        }
    }
}
