package com.anur.core.coder;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.anur.core.elect.vote.model.Votes;
import com.anur.exception.HanabiException;
import io.netty.util.internal.StringUtil;

/**
 * Created by Anur IjuoKaruKas on 1/24/2019
 */
public class Coder {

    private static final String REGEX = "-";

    private static Logger logger = LoggerFactory.getLogger(Coder.class);

    public static void main(String[] args) {
        Votes votes = new Votes();
        votes.setServerName("asdfasdfasdf");
        votes.setGeneration(111);

        String encode = Coder.encode(ProtocolEnum.VOTE, votes);

        Votes votes111 = Coder.decode(encode);

        System.out.println();
    }

    public static String encode(ProtocolEnum protocolEnum, Object obj) {
        return protocolEnum.name() + REGEX + JSON.toJSONString(obj);
    }

    public static <T> T decode(String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            throw new DecodeException("解码失败，从其他节点收到的请求为空：" + str);
        }

        int regexIndex = str.indexOf(REGEX);

        ProtocolEnum protocolEnum = Optional.of(str)
                                            .map(s -> s.substring(0, regexIndex))
                                            .map(ProtocolEnum::valueOf)
                                            .orElseThrow(() -> new DecodeException("解码失败，从其他节点收到的请求有误：" + str));

        Object obj = Optional.of(str)
                             .map(s -> s.substring(regexIndex + 1))
                             .map(s -> JSON.parseObject(s, protocolEnum.clazz))
                             .orElseThrow(() -> new DecodeException("解码失败，从其他节点收到的请求有误：" + str));

        return (T) obj;
    }

    public static class DecodeException extends HanabiException {

        public DecodeException(String message) {
            super(message);
        }
    }
}
