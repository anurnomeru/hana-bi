package com.anur.io.store.prelog;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.exception.HanabiException;
import com.anur.io.store.common.Operation;
import io.netty.buffer.ByteBuf;

/**
 * Created by Anur IjuoKaruKas on 2019/3/23
 */
public class ByteBufPreLogManager {

    private ConcurrentSkipListMap<Long, ByteBufPreLog> preLog;

    public void append(Operation operation) {
    }

    /**
     * 或者当前这一条之前的数据不包括这一条
     */
    public ByteBuf getBefore(GenerationAndOffset GAO) {
        long gen = GAO.getGeneration();
        long offset = GAO.getOffset();
        ConcurrentNavigableMap<Long, ByteBufPreLog> head = preLog.headMap(gen, true);

        if (head == null || head.size() == 0) {
            throw new HanabiException("获取预日志时：世代过小或者此世代还未有预日志");
        }

        ByteBufPreLog byteBufPreLog = head.firstEntry()
                                          .getValue();

        return byteBufPreLog.getBefore(offset);
    }

    /**
     * 丢弃掉一些消息（批量丢弃，但是不会丢弃掉当前这一条）
     */
    public void discardBefore(GenerationAndOffset GAO) {
        long gen = GAO.getGeneration();
        long offset = GAO.getOffset();
        ConcurrentNavigableMap<Long, ByteBufPreLog> head = preLog.headMap(gen, true);

        if (head == null || head.size() == 0) {
            throw new HanabiException("获取预日志时：世代过小或者此世代还未有预日志");
        }

        ByteBufPreLog byteBufPreLog = head.firstEntry()
                                          .getValue();
        byteBufPreLog.discardBefore(offset);
    }
}
