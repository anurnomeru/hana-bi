package com.anur.core.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.anur.core.lock.ReentrantReadWriteLocker;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 2/2/2019
 *
 * 管理channel，如果可以根据某个serverName获取到这个channel，代表是能连上服务端的
 */
public class ChannelManager extends ReentrantReadWriteLocker {

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

    public void register(String serverName, Channel channel) {
        this.writeLockSupplier(() -> serverChannelMap.put(serverName, channel));
    }

    public void unRegister(String serverName) {
        this.writeLockSupplier(() -> serverChannelMap.remove(serverName));
    }

    /**
     * 连接类型
     */
    public enum ChannelType {
        ELECT,
        COORDINATE;
    }
}
