package ink.anur.config

import ink.anur.config.common.ConfigurationEnum
import ink.anur.config.common.ConfigHelper

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 */
object ElectConfiguration : ConfigHelper() {

    fun getElectionTimeoutMs(): Int {
        return getConfig(ConfigurationEnum.ELECT_ELECTION_TIMEOUT_MS) { Integer.valueOf(it) } as Int
    }

    fun getVotesBackOffMs(): Int {
        return getConfig(ConfigurationEnum.ELECT_VOTES_BACK_OFF_MS) { Integer.valueOf(it) } as Int
    }

    fun getHeartBeatMs(): Int {
        return getConfig(ConfigurationEnum.ELECT_HEART_BEAT_MS) { Integer.valueOf(it) } as Int
    }
}