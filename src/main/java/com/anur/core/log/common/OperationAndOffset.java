package com.anur.core.log.common;

import com.anur.core.log.operation.Operation;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 */
public class OperationAndOffset {

    private Operation operation;

    private long offset;

    public OperationAndOffset(Operation operation, long offset) {
        this.operation = operation;
        this.offset = offset;
    }

    public long nextOffset() {
        return offset + 1;
    }

    public Operation getOperation() {
        return operation;
    }

    public long getOffset() {
        return offset;
    }
}
