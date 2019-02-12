package com.anur.core.elect.model;

/**
 * Created by Anur IjuoKaruKas on 2/12/2019
 */
public class HeartBeat {

    private String serverName;

    public HeartBeat() {
    }

    public HeartBeat(String serverName) {
        this.serverName = serverName;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }
}
