package com.anur.io.store.prelog;

import com.anur.io.store.common.Operation;
import io.netty.buffer.ByteBuf;

/**
 * Created by Anur IjuoKaruKas on 2019/3/21
 *
 * 还没有得到集群 n+1 同意的日志们
 */
public interface PreLogger {

    /**
     * 将消息追加到预日志中
     */
    void append(Operation operation, long offset);

    /**
     * 获取此消息之后的日志
     */
    ByteBuf getAfter(long offset, boolean inclusive);
}
