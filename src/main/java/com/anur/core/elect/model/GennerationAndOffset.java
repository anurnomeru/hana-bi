package com.anur.core.elect.model;

/**
 * Created by Anur IjuoKaruKas on 2019/3/12
 */
public class GennerationAndOffset {

    private long generation;

    private long offset;

    public GennerationAndOffset(long generation, long offset) {
        this.generation = generation;
        this.offset = offset;
    }

    public long getGeneration() {
        return generation;
    }

    public long getOffset() {
        return offset;
    }
}
