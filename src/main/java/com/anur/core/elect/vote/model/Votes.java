package com.anur.core.elect.vote.model;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 *
 * 选票相关
 */
public class Votes {

    /**
     * 该选票的世代信息
     */
    private int generation;

    /**
     * 投递该选票的服务名
     */
    private String serverName;

    /**
     * true  - 投票有效
     * false - 投票无效
     */
    private boolean active;

    public Votes() {
    }

    public Votes(int generation, String serverName, boolean active) {
        this.generation = generation;
        this.serverName = serverName;
        this.active = active;
    }

    public int getGeneration() {
        return generation;
    }

    public String getServerName() {
        return serverName;
    }

    public boolean isActive() {
        return active;
    }
}