package com.anur.core.coordinate.model;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
/**
 * Created by Anur IjuoKaruKas on 2019/3/27
 *
 * 消息发送后的回包
 */
public class Response {

    Consumer<ByteBuffer> callBack;

    public Response(Consumer<ByteBuffer> callBack) {
        this.callBack = callBack;
    }
}
