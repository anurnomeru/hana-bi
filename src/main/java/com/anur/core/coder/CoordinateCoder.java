package com.anur.core.coder;

import java.nio.charset.Charset;
import java.util.Optional;
import com.alibaba.fastjson.JSON;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.elect.ElectOperator;
import com.anur.exception.HanabiException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.StringUtil;

/**
 * Created by Anur IjuoKaruKas on 2/19/2019
 *
 * 集群内业务通讯的编解码器
 */
public class CoordinateCoder {

    private static final String REGEX = "★=Hana-★";

    private static final String SUFFIX = "\n";

    public static CoordinateDecodeWrapper decode(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            throw new ElectCoder.DecodeException("解码失败，从其他节点收到的请求为空：" + str);
        }

        String[] strs = str.split(REGEX);

        String operationId = Optional.of(strs[0])
                                     .map(String::valueOf)
                                     .orElseThrow(() -> new ElectCoder.DecodeException("解码失败，从其他节点收到请求的协议头 operationId 有误：" + str));

        CoordinateProtocolEnum coordinateProtocolEnum = Optional.of(strs[1])
                                                                .map(CoordinateProtocolEnum::valueOf)
                                                                .orElseThrow(() -> new ElectCoder.DecodeException("解码失败，从其他节点收到请求的协议头 protocolEnum 有误：" + str));

        long generation = Optional.of(strs[2])
                                  .map(Long::valueOf)
                                  .orElseThrow(() -> new ElectCoder.DecodeException("解码失败，从其他节点收到请求的协议头 generation 有误：" + str));

        String serverName = Optional.of(strs[3])
                                    .map(String::valueOf)
                                    .orElseThrow(() -> new ElectCoder.DecodeException("解码失败，从其他节点收到请求的协议头 generation 有误：" + str));

        return new CoordinateDecodeWrapper(operationId, coordinateProtocolEnum, generation, serverName, Optional.of(strs[4])
                                                                                                                .map(s -> JSON.parseObject(s, coordinateProtocolEnum.clazz))
                                                                                                                .orElseThrow(() -> new ElectCoder.DecodeException("解码失败，从其他节点收到请求的协议体有误：" + str)));
    }

    public static String encode(String operationId, CoordinateProtocolEnum coordinateProtocolEnum, Object obj) {
        if (obj == null) {
            throw new CoordinateCoderException("不能发送空协议内容");
        }

        if (!obj.getClass()
                .equals(coordinateProtocolEnum.clazz)) {
            throw new CoordinateCoderException(String.format("协议封装类型传入错误，应为 %s 但实际是 %s", coordinateProtocolEnum.clazz, obj.getClass()));
        }

        String json = JSON.toJSONString(obj);
        if (json.contains(REGEX)) {
            throw new HanabiException("协议封装失败，类中含有关键字：" + REGEX);
        }

        return
            operationId
                + REGEX + coordinateProtocolEnum.name()
                + REGEX + ElectOperator.getInstance()
                                       .getGeneration()
                + REGEX + InetSocketAddressConfigHelper.getServerName()
                + REGEX + json
                + SUFFIX;
    }

    public static ByteBuf encodeToByteBuf(String operationId, CoordinateProtocolEnum coordinateProtocolEnum, Object obj) {
        return Unpooled.copiedBuffer(encode(operationId, coordinateProtocolEnum, obj), Charset.defaultCharset());
    }

    public static class CoordinateDecodeWrapper {

        private String operationId;

        private CoordinateProtocolEnum coordinateProtocolEnum;

        private long generation;

        private String serverName;

        private Object object;

        public CoordinateDecodeWrapper(String operationId, CoordinateProtocolEnum coordinateProtocolEnum, long generation, String serverName, Object object) {
            this.operationId = operationId;
            this.coordinateProtocolEnum = coordinateProtocolEnum;
            this.generation = generation;
            this.serverName = serverName;
            this.object = object;
        }

        public CoordinateProtocolEnum getCoordinateProtocolEnum() {
            return coordinateProtocolEnum;
        }

        public long getGeneration() {
            return generation;
        }

        public String getServerName() {
            return serverName;
        }

        public String getOperationId() {
            return operationId;
        }

        public Object getObject() {
            return object;
        }
    }

    public static class CoordinateCoderException extends HanabiException {

        public CoordinateCoderException(String message) {
            super(message);
        }
    }
}
