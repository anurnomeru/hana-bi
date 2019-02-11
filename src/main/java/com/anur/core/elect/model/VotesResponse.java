package com.anur.core.elect.model;

/**
 * Created by Anur IjuoKaruKas on 1/25/2019
 *
 * 拉票结果
 */
public class VotesResponse extends Votes {

    /**
     * 拉票成功/失败
     */
    private boolean agreed;

    public VotesResponse(boolean agreed) {
        this.agreed = agreed;
    }

    public VotesResponse(long generation, String serverName, boolean agreed) {
        super(generation, serverName);
        this.agreed = agreed;
    }

    public boolean isAgreed() {
        return agreed;
    }
}
