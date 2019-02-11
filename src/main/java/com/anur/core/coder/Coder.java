package com.anur.core.coder;

import java.nio.charset.Charset;
import java.util.Optional;
import com.alibaba.fastjson.JSON;
import com.anur.exception.HanabiException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.StringUtil;

/**
 * Created by Anur IjuoKaruKas on 1/24/2019
 */
public class Coder {

    private static final String REGEX = "-";

    private static final String SUFFIX = "\n";

    public static DecodeWrapper decode(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            throw new DecodeException("解码失败，从其他节点收到的请求为空：" + str);
        }

        String[] strs = str.split(REGEX);

        ProtocolEnum protocolEnum = Optional.of(strs[0])
                                            .map(ProtocolEnum::valueOf)
                                            .orElseThrow(() -> new DecodeException("解码失败，从其他节点收到请求的协议头 protocolEnum 有误：" + str));

        return new DecodeWrapper(protocolEnum, Optional.of(strs[1])
                                                       .map(s -> JSON.parseObject(s, protocolEnum.clazz))
                                                       .orElseThrow(() -> new DecodeException("解码失败，从其他节点收到请求的协议体有误：" + str)));
    }

    public static String encode(ProtocolEnum protocolEnum, Object obj) {
        return protocolEnum.name() + REGEX
            + JSON.toJSONString(obj) + SUFFIX;
    }

    public static ByteBuf encodeToByteBuf(ProtocolEnum protocolEnum, Object obj) {
        return Unpooled.copiedBuffer(encode(protocolEnum, obj), Charset.defaultCharset());
    }

    public static class DecodeWrapper {

        public ProtocolEnum protocolEnum;

        public Object object;

        public DecodeWrapper(ProtocolEnum protocolEnum, Object object) {
            this.protocolEnum = protocolEnum;
            this.object = object;
        }
    }

    public static class DecodeException extends HanabiException {

        public DecodeException(String message) {
            super(message);
        }
    }
}
