package com.anur.io.core.coder;

import java.nio.charset.Charset;
import java.util.Optional;
import com.alibaba.fastjson.JSON;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.elect.ElectMeta;
import com.anur.core.elect.operator.ElectOperator;
import com.anur.exception.HanabiException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.StringUtil;

/**
 * Created by Anur IjuoKaruKas on 1/24/2019
 *
 * 选举通讯时使用的编解码器
 */
public class ElectCoder {

    private static final String REGEX = "★=Hana-★";

    private static final String SUFFIX = "\n";

    public static ElectDecodeWrapper decode(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            throw new DecodeException("解码失败，从其他节点收到的请求为空：" + str);
        }

        String[] strs = str.split(REGEX);

        ElectProtocolEnum protocolEnum = Optional.of(strs[0])
                                                 .map(ElectProtocolEnum::valueOf)
                                                 .orElseThrow(() -> new DecodeException("解码失败，从其他节点收到请求的协议头 protocolEnum 有误：" + str));

        long generation = Optional.of(strs[1])
                                  .map(Long::valueOf)
                                  .orElseThrow(() -> new DecodeException("解码失败，从其他节点收到请求的协议头 generation 有误：" + str));

        String serverName = Optional.of(strs[2])
                                    .map(String::valueOf)
                                    .orElseThrow(() -> new DecodeException("解码失败，从其他节点收到请求的协议头 generation 有误：" + str));

        return new ElectDecodeWrapper(protocolEnum, generation, serverName, Optional.of(strs[3])
                                                                                    .map(s -> JSON.parseObject(s, protocolEnum.clazz))
                                                                                    .orElseThrow(() -> new DecodeException("解码失败，从其他节点收到请求的协议体有误：" + str)));
    }

    public static String encode(ElectProtocolEnum protocolEnum, Object obj) {
        String json = JSON.toJSONString(obj);
        if (json.contains(REGEX)) {
            throw new HanabiException("协议封装失败，类中含有关键字：" + REGEX);
        }

        return protocolEnum.name() + REGEX
            + ElectMeta.INSTANCE
            .getGeneration() + REGEX
            + InetSocketAddressConfigHelper.Companion.getServerName() + REGEX
            + json + SUFFIX;
    }

    public static ByteBuf encodeToByteBuf(ElectProtocolEnum protocolEnum, Object obj) {
        return Unpooled.copiedBuffer(encode(protocolEnum, obj), Charset.defaultCharset());
    }

    public static class ElectDecodeWrapper {

        private ElectProtocolEnum protocolEnum;

        private long generation;

        private String serverName;

        private Object object;

        public ElectDecodeWrapper(ElectProtocolEnum protocolEnum, long generation, String serverName, Object object) {
            this.generation = generation;
            this.protocolEnum = protocolEnum;
            this.serverName = serverName;
            this.object = object;
        }

        public ElectProtocolEnum getProtocolEnum() {
            return protocolEnum;
        }

        public long getGeneration() {
            return generation;
        }

        public String getServerName() {
            return serverName;
        }

        public Object getObject() {
            return object;
        }
    }

    public static class DecodeException extends RuntimeException {

        public DecodeException(String message) {
            super(message);
        }
    }
}
