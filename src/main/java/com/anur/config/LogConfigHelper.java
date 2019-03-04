package com.anur.config;

/**
 * Created by Anur IjuoKaruKas on 2019/3/4
 */
public class LogConfigHelper extends ConfigHelper {

    public static int getIndexInterval() {
        return getConfig(ConfigEnum.LOG_INDEX_INTERVAL, Integer::valueOf);
    }

    public static int getMaxIndexSize() {
        return getConfig(ConfigEnum.LOG_MAX_INDEX_SIZE, Integer::valueOf);
    }

    public static int getMaxLogMessageSize() {
        return getConfig(ConfigEnum.LOG_MAX_MESSAGE_SIZE, Integer::valueOf);
    }
}
