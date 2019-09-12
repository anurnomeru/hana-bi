package com.anur.core.coordinate.model;

import java.util.List;

/**
 * Created by Anur IjuoKaruKas on 2019/3/26
 */
public class Cluster {

    private String leader;

    private List<HanabiNode> clusters;

    public Cluster(String leader, List<HanabiNode> clusters) {
        this.leader = leader;
        this.clusters = clusters;
    }

    public String getLeader() {
        return leader;
    }

    public List<HanabiNode> getClusters() {
        return clusters;
    }
}
