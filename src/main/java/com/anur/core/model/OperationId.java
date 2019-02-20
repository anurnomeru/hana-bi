package com.anur.core.model;

/**
 * Created by Anur IjuoKaruKas on 2/20/2019
 *
 * OperationId 的规则为  G世代S流水号N服务hash码
 *
 * 同世代下，流水号的顺序代表了操作的顺序
 * 不同世代下，世代的顺序代表了操作的顺序
 *
 * 服务器hash码用于拓展后面项目改成多主多从模式
 */
public class OperationId {

    private long generation;

    private long serial;

    private int hash;

    public OperationId(long generation, long serial, int hash) {
        this.generation = generation;
        this.serial = serial;
        this.hash = hash;
    }

    public long getGeneration() {
        return generation;
    }

    public long getSerial() {
        return serial;
    }

    public int getHash() {
        return hash;
    }

    @Override
    public String toString() {
        return "OperationId{" +
            "generation=" + generation +
            ", serial=" + serial +
            ", hash=" + hash +
            '}';
    }
}
