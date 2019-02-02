package com.anur.core.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 2/2/2019
 *
 * 管理channel，如果可以根据某个serverName获取到这个channel，代表是能连上服务端的
 */
public class ChannelManager {

    private Map<String/* serverName */, Channel> serverChannelMap;

    private Lock readLock;

    private Lock writeLock;

    public ChannelManager() {
        this.serverChannelMap = new HashMap<>();
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        readLock = readWriteLock.readLock();
        writeLock = readWriteLock.writeLock();
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
        Channel channel;
        try {
            readLock.lock();
            channel = serverChannelMap.get(serverName);
        } finally {
            readLock.unlock();
        }
        return channel;
    }

    public void register(String serverName, Channel channel) {
        try {
            writeLock.lock();
            serverChannelMap.put(serverName, channel);
        } finally {
            writeLock.unlock();
        }
    }

    public void unRegister(String serverName) {
        try {
            writeLock.lock();
            serverChannelMap.remove(serverName);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 连接类型
     */
    public enum ChannelType {
        ELECT;
    }
}
