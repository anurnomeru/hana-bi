package com.anur.core.coder;

import com.anur.core.model.OperationId;
import com.anur.core.util.OperationIdGenerator;

/**
 * Created by Anur IjuoKaruKas on 2/20/2019
 */
public class TestOperationIdGenerator {

    public static void main(String[] args) {
        String operId = OperationIdGenerator.genOperationId(0L, 0L);
        System.out.println(operId);

        OperationId operationId = OperationIdGenerator.decodeOperationId(operId);
        System.out.println(operationId.toString());
    }
}