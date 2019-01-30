package com.anur.core.coder;

import java.util.Optional;
import com.alibaba.fastjson.JSON;
import com.anur.exception.HanabiException;
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

        int regexIndex = str.indexOf(REGEX);

        ProtocolEnum protocolEnum = Optional.of(str)
                                            .map(s -> s.substring(0, regexIndex))
                                            .map(ProtocolEnum::valueOf)
                                            .orElseThrow(() -> new DecodeException("解码失败，从其他节点收到请求的协议头 protocolEnum 有误：" + str));

        return new DecodeWrapper(protocolEnum, Optional.of(str)
                                                       .map(s -> s.substring(regexIndex + 1))
                                                       .map(s -> JSON.parseObject(s, protocolEnum.clazz))
                                                       .orElseThrow(() -> new DecodeException("解码失败，从其他节点收到请求的协议体有误：" + str)));
    }

    public static String encode(ProtocolEnum protocolEnum, Object obj) {
        return protocolEnum.name() + REGEX + JSON.toJSONString(obj) + SUFFIX;
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
