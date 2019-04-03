package com.anur.core.struct.coordinate;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.AbstractTimedStruct;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.internal.StringUtil;

/**
 * Created by Anur IjuoKaruKas on 2019/3/27
 *
 * 用于向协调 Leader 注册自己
 */
public class Register extends AbstractTimedStruct {

    public static final int SizeOffset = TimestampOffset + TimestampLength;

    public static final int SizeLength = 4;

    public static final int ContentOffset = SizeOffset + SizeLength;

    public final String serverName;

    public Register(String serverName) {
        assert (!StringUtil.isNullOrEmpty(serverName));
        this.serverName = serverName;

        byte[] bytes = serverName.getBytes(Charset.defaultCharset());
        int size = bytes.length;
        ByteBuffer byteBuffer = ByteBuffer.allocate(ContentOffset + size);
        init(byteBuffer, OperationTypeEnum.REGISTER);

        byteBuffer.putInt(size);
        byteBuffer.put(bytes);
        byteBuffer.flip();
    }

    public Register(ByteBuffer byteBuffer) {
        buffer = byteBuffer;
        int size = buffer.getInt(SizeOffset);

        buffer.position(ContentOffset);
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        this.serverName = new String(bytes);

        byteBuffer.rewind();
        byteBuffer.limit(ContentOffset);
    }

    public String getServerName() {
        return serverName;
    }

    @Override
    public void writeIntoChannel(Channel channel) {
        channel.write(Unpooled.wrappedBuffer(buffer));
    }

    @Override
    public int totalSize() {
        return size();
    }
}
