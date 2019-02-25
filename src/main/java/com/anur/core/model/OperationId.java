package com.anur.core.model;

/**
 * Created by Anur IjuoKaruKas on 2/20/2019
 *
 * OperationId 的规则为  G世代S流水号
 *
 * 同世代下，流水号的顺序代表了操作的顺序
 * 不同世代下，世代的顺序代表了操作的顺序
 */
public class OperationId {

    private long generation;

    private long serial;


    public OperationId(long generation, long serial) {
        this.generation = generation;
        this.serial = serial;
    }

    public long getGeneration() {
        return generation;
    }

    public long getSerial() {
        return serial;
    }


    @Override
    public String toString() {
        return "OperationId{" +
            "generation=" + generation +
            ", serial=" + serial +
            '}';
    }
}
