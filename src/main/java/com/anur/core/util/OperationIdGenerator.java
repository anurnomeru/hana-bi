package com.anur.core.util;

import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.core.model.OperationId;

/**
 * Created by Anur IjuoKaruKas on 2/20/2019
 */
public class OperationIdGenerator {

    public static String genOperationId(long generation, long serial) {
        return new StringBuilder("G").append(generation)
                                     .append("S")
                                     .append(serial)
                                     .append("N")
                                     .append(InetSocketAddressConfigHelper.getServerName()
                                                                          .hashCode())
                                     .toString();
    }

    public static OperationId decodeOperationId(String operationId) {
        return new OperationId(
            Long.valueOf(operationId.substring(1, operationId.indexOf("S"))),
            Long.valueOf(operationId.substring(operationId.indexOf("S") + 1, operationId.indexOf("N"))),
            Integer.valueOf(operationId.substring(operationId.indexOf("N") + 1))
        );
    }
}
