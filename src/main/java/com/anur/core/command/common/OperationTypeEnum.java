package com.anur.core.command.common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2019/3/11
 */
public enum OperationTypeEnum {
    NONE(-1),// 无

    SETNX(1),// 请求 HASH SET

    // 10000 开始的命令都是集群协调命令，不需要记录日志
    REGISTER(10000),// 协调从节点向主节点注册
    ;

    public int byteSign;

    OperationTypeEnum(int byteSign) {
        this.byteSign = byteSign;
    }

    private static Map<Integer, OperationTypeEnum> byteSignMap = new HashMap<>();

    static {
        Set<Integer> unique = new HashSet<>();
        for (OperationTypeEnum value : OperationTypeEnum.values()) {
            if (!unique.add(value.byteSign)) {
                throw new HanabiException("OperationTypeEnum 中，byteSign 不可重复。");
            }

            byteSignMap.put(value.byteSign, value);
        }
    }

    public static OperationTypeEnum parseByByteSign(int byteSign) {
        return byteSignMap.get(byteSign);
    }
}
