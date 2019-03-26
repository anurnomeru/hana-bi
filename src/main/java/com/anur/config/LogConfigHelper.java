package com.anur.config;

/**
 * Created by Anur IjuoKaruKas on 2019/3/4
 */
public class LogConfigHelper extends ConfigHelper {

    private static String relativelyPath;

    public static String getBaseDir() {
        if (relativelyPath == null) {// 并发没关系，这个并不会变动
            relativelyPath = System.getProperty("user.dir");
        }
        return relativelyPath + "\\" + InetSocketAddressConfigHelper.getServerName();
    }

    public static int getIndexInterval() {
        return getConfig(ConfigEnum.LOG_INDEX_INTERVAL, Integer::valueOf);
    }

    public static int getMaxIndexSize() {
        return getConfig(ConfigEnum.LOG_MAX_INDEX_SIZE, Integer::valueOf);
    }

    public static int getMaxLogMessageSize() {
        return getConfig(ConfigEnum.LOG_MAX_MESSAGE_SIZE, Integer::valueOf);
    }

    public static int getMaxLogSegmentSize() {
        return getConfig(ConfigEnum.LOG_MAX_SEGMENT_SIZE, Integer::valueOf);
    }
}
