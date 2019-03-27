package com.anur.core.coordinate.model;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Created by Anur IjuoKaruKas on 2019/3/27
 *
 * 消息发送后的回调
 */
public class Response {

    private boolean complate;

    private Consumer<ByteBuffer> callBack;

    public Response(Consumer<ByteBuffer> callBack) {
        this.callBack = callBack;
    }

    public boolean isComplate() {
        return complate;
    }
}
