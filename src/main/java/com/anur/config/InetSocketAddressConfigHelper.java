package com.anur.config;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import com.anur.util.ConfigHelper;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 *
 * 网络相关配置，都可以从这里获取
 */
public class InetSocketAddressConfigHelper extends ConfigHelper {

    public void refreshConfig() {
        ConfigHelper.refresh();
    }

    public static Integer getServerPort() {
        return getConfig(ConfigEnum.SERVER_PORT, Integer::valueOf);
    }

    public static String getServerName() {
        return getConfig(ConfigEnum.SERVER_NAME, Function.identity());
    }

    public static List<HanabiCluster> getCluster() {
        return getConfigSimilar(ConfigEnum.CLIENT_ADDR, pair -> {
            String serverName = pair.getKey();
            String[] split = pair.getValue()
                                 .split(":");
            return new HanabiCluster(serverName, split[0], Integer.valueOf(split[1]), Integer.valueOf(split[2]));
        });
    }

    /**
     * 保存了初始化连接另一个客户端需要哪个地址、哪些端口
     */
    public static class HanabiCluster {

        private String serverName;

        private String host;

        private int serverPort;

        private int clientPort;

        public HanabiCluster(String serverName, String host, int serverPort, int clientPort) {
            this.serverName = serverName;
            this.host = host;
            this.serverPort = serverPort;
            this.clientPort = clientPort;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HanabiCluster that = (HanabiCluster) o;
            return Objects.equals(serverName, that.serverName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(serverName);
        }

        public String getServerName() {
            return serverName;
        }

        public String getHost() {
            return host;
        }

        public int getServerPort() {
            return serverPort;
        }

        public int getClientPort() {
            return clientPort;
        }
    }
}
