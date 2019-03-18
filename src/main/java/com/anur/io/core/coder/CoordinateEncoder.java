package com.anur.io.core.coder;

import java.nio.ByteBuffer;
import com.anur.config.LogConfigHelper;
import com.anur.core.util.Crc32;
import com.anur.exception.HanabiException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by Anur IjuoKaruKas on 2019/3/14
 *
 * 本来准备使用 MessageToByteEncoder，但是这个类貌似无法和零拷贝结合在一起，故采用工具类的形式来进行协议封装
 */
public class CoordinateEncoder {

    private static int CrcLength = 4;

    private static int MsgLength = 4;

    public static void calcCrcAndFlushMsg(Channel channel, ByteBuffer body) {
        ByteBuf head = calcCrc(body);

        // 避免同个 channel 发生多线程问题
        synchronized (channel) {
            channel.writeAndFlush(head);
            channel.writeAndFlush(Unpooled.wrappedBuffer(body));
        }
    }

    private static ByteBuf calcCrc(ByteBuffer body) {
        int limit = body.limit();
        if (limit > LogConfigHelper.getMaxLogMessageSize()) {
            throw new HanabiException("集群协调消息的大小不能大于" + LogConfigHelper.getMaxLogMessageSize());
        }

        ByteBuf head = Unpooled.buffer(CrcLength + MsgLength);

        head.writeInt(CrcLength + limit);
        head.writeInt(getUnsignedInt(Crc32.crc32(body.array())));
        return head;
    }

    private static int getUnsignedInt(long v) {
        return (int) (v & 0xffffffffL);
    }
}
