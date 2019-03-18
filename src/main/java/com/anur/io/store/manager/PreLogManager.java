package com.anur.io.store.manager;

import java.nio.ByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Created by Anur IjuoKaruKas on 2019/3/18
 */
public class PreLogManager {

    private final ByteBuf byteBuf;

    public PreLogManager() {
        this.byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(ByteBuffer.allocate(1));
    }
}
