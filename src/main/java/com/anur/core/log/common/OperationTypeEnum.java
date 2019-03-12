package com.anur.core.log.common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2019/3/11
 */
public enum OperationTypeEnum {
    SETNX(0),
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

    public static OperationTypeEnum parseBybyteSign(int byteSign) {
        return byteSignMap.get(byteSign);
    }

}
