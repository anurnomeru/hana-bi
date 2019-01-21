package com.anur.config.bootstrap;

import java.util.function.Function;
import com.anur.util.ConfigHelper;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 */
public class BootStrapConfigHelper extends ConfigHelper {

    public static Integer getServerProt() {
        return getConfig(BootStrapConfigConstant.SERVER_PORT, Integer::valueOf);
    }

    public static String getServerName() {
        return getConfig(BootStrapConfigConstant.SERVER_NAME, Function.identity());
    }

    public static String getClientPort() {
        return getConfig(BootStrapConfigConstant.CLIENT_ADDR, Function.identity());
    }

    public static void main(String[] args) {
        System.out.println(getServerProt());
        System.out.println(getServerName());
        System.out.println(getClientPort());
    }

    public static class BootStrapConfigConstant {

        public static final String SERVER_PORT = "server.port";

        public static final String SERVER_NAME = "server.name";

        public static final String CLIENT_ADDR = "client.addr";
    }
}
