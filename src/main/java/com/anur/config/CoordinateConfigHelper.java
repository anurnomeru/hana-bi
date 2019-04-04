package com.anur.config;

/**
 * Created by Anur IjuoKaruKas on 4/4/2019
 */
public class CoordinateConfigHelper extends ConfigHelper {

    public static int getFetchBackOfMs() {
        return getConfig(ConfigEnum.COORDINATE_FETCH_BACK_OFF_MS, Integer::valueOf);
    }
}
