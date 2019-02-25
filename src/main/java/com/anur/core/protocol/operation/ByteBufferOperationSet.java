package com.anur.core.protocol.operation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 */
public class ByteBufferOperationSet extends OperationSet {

    private ByteBuffer byteBuffer;

    public ByteBufferOperationSet(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public int writeFullyTo(GatheringByteChannel gatheringByteChannel) throws IOException {
        byteBuffer.mark();
        int written = 0;
        while (written < sizeInBytes()) {
            written += gatheringByteChannel.write(byteBuffer);
        }
        byteBuffer.reset();// mark和reset，为了将byteBuffer的指针还原到写之前的位置
        return written;
    }

    /**
     * Returns this buffer's limit.
     */
    public int sizeInBytes() {
        return byteBuffer.limit();
    }

    @Override
    int writeTo(GatheringByteChannel channel, long offset, int maxSize) {
        return 0;
    }

    @Override
    Iterator<OperationAndOffset> iterator() {
        return null;
    }
}
