/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.anur.io.store.operationset;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;
import com.anur.exception.HanabiException;
import com.anur.io.store.common.Operation;
import com.anur.io.store.common.OperationAndOffset;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 *
 * 仿照 Kafka ByteBufferMessageSet 所写
 */
public class ByteBufferOperationSet extends OperationSet {

    private ByteBuffer byteBuffer;

    public static final ByteBufferOperationSet Empty = new ByteBufferOperationSet(ByteBuffer.allocate(0));

    /**
     * 一个日志将要被append到日志之前，需要进行的操作
     */
    public ByteBufferOperationSet(Operation operation, long offset) {
        int size = operation.size();

        ByteBuffer byteBuffer = ByteBuffer.allocate(size + LogOverhead);
        byteBuffer.putLong(offset);
        byteBuffer.putInt(size);
        byteBuffer.put(operation.getByteBuffer());

        byteBuffer.rewind();
        this.byteBuffer = byteBuffer;
    }

    public static void main(String[] args) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(12);
        byteBuffer.putInt(0);
        byteBuffer.putInt(0);
        byteBuffer.putInt(0);

        File file = new File("C:\\Users\\Administrator\\Desktop\\test.log");
        file.createNewFile();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");

        int w = 0;
        while (w < 12) {
            w += randomAccessFile.getChannel()
                                 .write(byteBuffer);
        }
    }

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

    @Override
    public int writeTo(GatheringByteChannel channel, long offset, int maxSize) throws IOException {
        if (offset > Integer.MAX_VALUE) {
            throw new HanabiException("offset 不应大于 Integer.MaxValue");
        }
        ByteBuffer dup = byteBuffer.duplicate();
        int pos = (int) offset;
        dup.position(pos);
        dup.limit(Math.min(dup.limit(), pos + maxSize));
        return channel.write(dup);
    }

    /**
     * Returns this buffer's limit.
     */
    public int sizeInBytes() {
        return byteBuffer.limit();
    }

    @Override
    public Iterator<OperationAndOffset> iterator() {
        return null;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }
}


