package com.anur.util;

import java.util.ResourceBundle;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 */
public class ConfigHelper {

    protected volatile static ResourceBundle RESOURCE_BUNDLE;

    static {
        RESOURCE_BUNDLE = ResourceBundle.getBundle("application");
    }

    public static void refresh() {
        synchronized (ConfigHelper.class) {
            RESOURCE_BUNDLE = ResourceBundle.getBundle("application");
        }
    }
}
