package com.anur.core.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import com.anur.core.lock.ReentrantReadWriteLocker;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 2/2/2019
 *
 * 管理channel，如果可以根据某个serverName获取到这个channel，代表是能连上服务端的
 */
public class ChannelManager extends ReentrantReadWriteLocker {

    public static final String CoordinateLeaderSign = "Leader";

    private Map<String/* serverName */, Channel> serverChannelMap;

    public ChannelManager() {
        this.serverChannelMap = new HashMap<>();
    }

    private static Map<ChannelType, ChannelManager> MANAGER_MAP = new ConcurrentHashMap<>();

    public static ChannelManager getInstance(ChannelType channelType) {
        ChannelManager channelManager = MANAGER_MAP.get(channelType);
        if (channelManager == null) {
            synchronized (ChannelManager.class) {
                channelManager = MANAGER_MAP.get(channelType);
                if (channelManager == null) {

                    channelManager = new ChannelManager();
                    MANAGER_MAP.put(channelType, channelManager);
                }
            }
        }

        return channelManager;
    }

    /**
     * 如果还没连接上服务，是会返回空的
     */
    public Channel getChannel(String serverName) {
        return this.readLockSupplier(() -> serverChannelMap.get(serverName));
    }

    /**
     * 向 channelManager 注册服务
     */
    public void register(String serverName, Channel channel) {
        this.writeLockSupplier(() -> serverChannelMap.put(serverName, channel));
    }

    /**
     * 根据服务名来注销服务
     */
    public void unRegister(String serverName) {
        this.writeLockSupplier(() -> serverChannelMap.remove(serverName));
    }

    /**
     * 根据管道来注销服务
     */
    public void unRegister(Channel channel) {
        this.writeLockSupplier(() -> {
            for (Entry<String, Channel> e : serverChannelMap.entrySet()) {
                if (e.getValue()
                     .equals(channel)) {
                    this.unRegister(e.getKey());
                    break;
                }
            }
            return null;
        });
    }

    /**
     * 连接类型
     */
    public enum ChannelType {
        ELECT,
        COORDINATE;
    }
}
