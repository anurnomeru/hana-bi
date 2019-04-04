package com.anur.config;

/**
 * Created by Anur IjuoKaruKas on 4/4/2019
 */
public class ElectConfigHelper extends ConfigHelper {

    public static int getElectionTimeoutMs() {
        return getConfig(ConfigEnum.ELECT_ELECTION_TIMEOUT_MS, Integer::valueOf);
    }

    public static int getVotesBackOffMs() {
        return getConfig(ConfigEnum.ELECT_VOTES_BACK_OFF_MS, Integer::valueOf);
    }

    public static int getHeartBeatMs() {
        return getConfig(ConfigEnum.ELECT_HEART_BEAT_MS, Integer::valueOf);
    }
}
