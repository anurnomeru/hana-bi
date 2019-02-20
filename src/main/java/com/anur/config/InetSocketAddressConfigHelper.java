package com.anur.config;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 *
 * 网络相关配置，都可以从这里获取
 */
public class InetSocketAddressConfigHelper extends ConfigHelper {

    private static HanabiNode me;

    static {
        me = getNode(getConfig(ConfigEnum.SERVER_NAME, Function.identity()));
    }

    public void refreshConfig() {
        ConfigHelper.refresh();
    }

    public static Integer getServerElectionPort() {
        return me.electionPort;
    }

    public static Integer getServerCoordinatePort() {
        return me.coordinatePort;
    }

    public static String getServerName() {
        return me.serverName;
    }

    public static List<HanabiNode> getCluster() {
        return getConfigSimilar(ConfigEnum.CLIENT_ADDR, pair -> {
            String serverName = pair.getKey();
            String[] split = pair.getValue()
                                 .split(":");
            return new HanabiNode(serverName, split[0], Integer.valueOf(split[1]), Integer.valueOf(split[2]));
        });
    }

    public static HanabiNode getNode(String serverName) {
        return getCluster().stream()
                           .collect(Collectors.toMap(o -> o.serverName, Function.identity()))
                           .getOrDefault(serverName, HanabiNode.NOT_EXIST);
    }

    /**
     * 保存了初始化连接另一个客户端需要哪个地址、哪些端口
     */
    public static class HanabiNode {

        public static final HanabiNode NOT_EXIST = new HanabiNode("", "", 0, 0);

        private String serverName;

        private String host;

        private int electionPort;

        private int coordinatePort;

        public HanabiNode(String serverName, String host, int electionPort, int coordinatePort) {
            this.serverName = serverName;
            this.host = host;
            this.electionPort = electionPort;
            this.coordinatePort = coordinatePort;
        }

        /**
         * 是否是本地节点
         */
        public boolean isLocalNode() {
            return this.serverName.equals(InetSocketAddressConfigHelper.getServerName());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HanabiNode that = (HanabiNode) o;
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

        public int getElectionPort() {
            return electionPort;
        }

        public int getCoordinatePort() {
            return coordinatePort;
        }
    }
}
