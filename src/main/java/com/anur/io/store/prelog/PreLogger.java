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
    void append(Operation operation);

    /**
     * 获取此消息之后的消息（不包括 targetOffset 这一条）
     */
    ByteBuf getAfter(long offset);

    /**
     * 获获取此消息之前的消息（包括 targetOffset 这一条）
     */
    ByteBuf getBefore(long offset);

    /**
     * 清除之前的 ByteBuf
     */
    void discardBefore(long offset);
}
