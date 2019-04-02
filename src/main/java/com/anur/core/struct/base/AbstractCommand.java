package com.anur.core.struct.base;

import java.nio.ByteBuffer;
import com.anur.core.util.ByteBufferUtil;
import com.anur.exception.HanabiException;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 2019/3/28
 *
 * 一个 Command 由以下部分组成：
 *
 * 　4　   +   4    + ...（子类自由扩展）
 * CRC32  +  type  + ...（子类自由扩展）
 *
 * 所有的指令都满足 4位CRC + 4位类型
 */
public abstract class AbstractCommand {

    public static final int CrcOffset = 0;

    public static final int CrcLength = 4;

    public static final int TypeOffset = CrcOffset + CrcLength;

    public static final int TypeLength = 4;

    // =================================================================

    protected ByteBuffer buffer;

    public ByteBuffer getByteBuffer() {
        return buffer;
    }

    public int size() {
        return buffer.limit();
    }

    public void ensureValid() {
        long stored = checkSum();
        long compute = computeChecksum();
        if (stored != compute) {
            throw new HanabiException(String.format("Message is corrupt (stored crc = %s, computed crc = %s)", stored, compute));
        }
    }

    public long checkSum() {
        return ByteBufferUtil.readUnsignedInt(buffer, CrcOffset);
    }

    public long computeChecksum() {
        return ByteBufferUtil.crc32(buffer.array(), buffer.arrayOffset() + TypeOffset, buffer.limit() - TypeOffset);
    }

    public OperationTypeEnum getOperationTypeEnum() {
        return OperationTypeEnum.parseByByteSign(buffer.getInt(TypeOffset));
    }

    /**
     * 如何写入 Channel
     */
    public abstract void writeIntoChannel(Channel channel);

    /**
     * 真正的 size，并不局限于维护的 buffer
     */
    public abstract int totalSize();
}
