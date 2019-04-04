package com.anur.core.coordinate.sender;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.struct.base.AbstractStruct;
import com.anur.core.util.ChannelManager;
import com.anur.core.util.ChannelManager.ChannelType;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 2019/3/14
 *
 * 本来准备使用 MessageToByteEncoder，但是这个类貌似无法和零拷贝结合在一起，故采用工具类的形式来进行协议封装
 */
public class CoordinateSender {

    private static Logger logger = LoggerFactory.getLogger(CoordinateSender.class);

    private static Map<String, ReentrantLock> lockerMap = new HashMap<>();

    /**
     * 在向同一个服务发送东西时需要加锁
     */
    private static ReentrantLock getLock(String serverName) {
        synchronized (CoordinateSender.class) {
            ReentrantLock lock = lockerMap.get(serverName);
            if (lock == null) {
                lock = new ReentrantLock();
                lockerMap.put(serverName, lock);
            }
            return lock;
        }
    }

    /**
     * 向某个服务发送东西~
     */
    public static void send(String serverName, AbstractStruct body) {
        // 避免同个 channel 发生多线程问题
        synchronized (getLock(serverName)) {
            logger.debug("正向节点发送 {} 关于 {} 的 request", serverName, body.getOperationTypeEnum()
                                                                   .name());
            Channel channel = ChannelManager.getInstance(ChannelType.COORDINATE)
                                            .getChannel(serverName);

            channel.write(Unpooled.copyInt(body.totalSize()));
            body.writeIntoChannel(channel);
            channel.flush();
        }
    }
}
