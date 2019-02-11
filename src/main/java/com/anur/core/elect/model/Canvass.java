package com.anur.core.elect.model;

/**
 * Created by Anur IjuoKaruKas on 1/25/2019
 *
 * 拉票结果
 */
public class Canvass {

    /**
     * 该选票的世代信息
     */
    private long generation;

    /**
     * 拉票成功/失败
     */
    private boolean agreed;

    public Canvass() {
    }

    public Canvass(long generation, boolean agreed) {
        this.generation = generation;
        this.agreed = agreed;
    }

    public long getGeneration() {
        return generation;
    }

    public void setGeneration(int generation) {
        this.generation = generation;
    }

    public boolean isAgreed() {
        return agreed;
    }

    public void setAgreed(boolean agreed) {
        this.agreed = agreed;
    }
}
