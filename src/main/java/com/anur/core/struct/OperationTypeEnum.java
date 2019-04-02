package com.anur.core.struct;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import com.anur.core.struct.base.AbstractCommand;
import com.anur.core.struct.base.Operation;
import com.anur.core.struct.base.Register;
import com.anur.core.struct.coordinate.FetchResponse;
import com.anur.core.struct.coordinate.Fetcher;
import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2019/3/11
 */
public enum OperationTypeEnum {
    /**
     * 无类型
     */
    NONE(-1, null),

    /**
     * HASH_SET 的 set nx
     */
    SETNX(1, Operation.class),

    // WARN： 10000 开始的命令都是集群协调命令，不需要记录日志

    /**
     * 协调从节点向主节点注册
     */
    REGISTER(10000, Register.class),

    /**
     * 协调节点从主节点拉取消息
     */
    FETCH(10001, Fetcher.class),// 拉取消息

    /**
     * 协调节点从主节点拉取消息的回复，返回拉取的消息
     */
    FETCH_RESPONSE(10002, FetchResponse.class),// 返回拉取的消息
    ;

    public int byteSign;

    public Class<? extends AbstractCommand> clazz;

    OperationTypeEnum(int byteSign, Class<? extends AbstractCommand> clazz) {
        this.clazz = clazz;
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
