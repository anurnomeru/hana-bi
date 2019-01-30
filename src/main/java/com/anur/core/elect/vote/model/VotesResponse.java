package com.anur.core.elect.vote.model;

/**
 * Created by Anur IjuoKaruKas on 1/30/2019
 *
 * 收到拉票请求后的回复
 */
public class VotesResponse extends Votes {

    private boolean active;

    public VotesResponse(int generation, String serverName, boolean active) {
        super(generation, serverName);
        this.active = active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isActive() {
        return active;
    }
}
