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

    /**
     * 记录了服务名和 channel 的映射
     */
    private Map<String/* serverName */, Channel> serverChannelMap;

    /**
     * 记录了 channel 和服务名的映射
     */
    private Map<Channel, String/* serverName */> channelServerMap;

    public ChannelManager() {
        this.serverChannelMap = new HashMap<>();
        this.channelServerMap = new HashMap<>();
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
     * 如果还没连接上服务，是会返回空的
     *
     * TODO 如果后期服务多了最好改成双向映射，当然就连接十几台机器，循环几次也没关系
     */
    public String getChannelName(Channel channel) {
        return this.readLockSupplier(() -> channelServerMap.get(channel));
    }

    /**
     * 向 channelManager 注册服务
     */
    public void register(String serverName, Channel channel) {
        this.writeLockSupplier(() -> {
            serverChannelMap.put(serverName, channel);
            channelServerMap.put(channel, serverName);
            return null;
        });
    }

    /**
     * 根据服务名来注销服务
     */
    public void unRegister(String serverName) {
        this.writeLockSupplier(() -> {
            Channel channel = serverChannelMap.remove(serverName);
            channelServerMap.remove(channel);
            return null;
        });
    }

    /**
     * 根据管道来注销服务
     */
    public void unRegister(Channel channel) {
        this.writeLockSupplier(() -> {
            String serverName = channelServerMap.remove(channel);
            serverChannelMap.remove(serverName);
            return null;
        });
    }

    /**
     * 连接类型
     */
    public enum ChannelType {
        ELECT,
        COORDINATE,
    }
}
