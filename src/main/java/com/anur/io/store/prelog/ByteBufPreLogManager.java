package com.anur.io.store.prelog;

import java.util.concurrent.ConcurrentSkipListMap;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.io.store.common.Operation;
import io.netty.buffer.ByteBuf;

/**
 * Created by Anur IjuoKaruKas on 2019/3/23
 */
public class ByteBufPreLogManager implements PreLogger {

    private ConcurrentSkipListMap<Long, ByteBufPreLog> preLog;

    @Override
    public void append(Operation operation) {
    }

    @Override
    public ByteBuf getAfter(GenerationAndOffset GAO) {
        return null;
    }

    @Override
    public ByteBuf getBefore(GenerationAndOffset GAO) {
        return null;
    }

    @Override
    public void discardBefore(GenerationAndOffset GAO) {

    }
}
