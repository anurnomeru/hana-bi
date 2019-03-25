package com.anur.io.store.prelog;

import com.anur.io.store.common.Operation;
import io.netty.buffer.ByteBuf;

/**
 * Created by Anur IjuoKaruKas on 2019/3/23
 */
public class ByteBufPreLogManager implements PreLogger {

    @Override
    public void append(Operation operation) {
    }

    @Override
    public ByteBuf getAfter(long offset) {
        return null;
    }

    @Override
    public ByteBuf getBefore(long offset) {
        return null;
    }

    @Override
    public void discardBefore(long offset) {

    }
}
