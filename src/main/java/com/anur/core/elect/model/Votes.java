package com.anur.core.elect.model;

import java.util.Objects;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 *
 * 选票内容
 */
public class Votes {

    /**
     * 该选票的世代信息
     */
    private long generation;

    /**
     * 投递该选票的节点名
     */
    private String serverName;

    public Votes() {
    }

    public Votes(long generation, String serverName) {
        this.generation = generation;
        this.serverName = serverName;
    }

    public long getGeneration() {
        return generation;
    }

    public String getServerName() {
        return serverName;
    }

    public void setGeneration(int generation) {
        this.generation = generation;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Votes votes = (Votes) o;
        return generation == votes.generation &&
            Objects.equals(serverName, votes.serverName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generation, serverName);
    }

    @Override
    public String toString() {
        return "Votes{" +
            "generation=" + generation +
            ", serverName='" + serverName + '\'' +
            '}';
    }
}
