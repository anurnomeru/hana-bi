package com.anur.config.bootstrap;

import java.util.Optional;
import com.anur.util.ConfigHelper;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 */
public class BootStrapConfigHelper extends ConfigHelper {

//    public static Integer getServerProt() {
//        Optional.ofNullable(RESOURCE_BUNDLE.getString(BootStrapConfigConstant.SERVER_PORT))
//                .map(port -> Integer.valueOf(port))
//                .orElseThrow();
//    }

    public static class BootStrapConfigConstant {

        public static final String SERVER_PORT = "server.port";

        public static final String CLIENT_ADDR = "client.addr";
    }

}
