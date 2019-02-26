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

package com.anur.core.log.operation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019\
 */
public class ByteBufferOperationSet extends OperationSet {

    private ByteBuffer byteBuffer;

    private static final ByteBufferOperationSet Empty = new ByteBufferOperationSet(ByteBuffer.allocate(0));

    public ByteBufferOperationSet(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    private ByteBuffer create(OffsetAssigner offsetAssigner, Collection<Operation> operationCollection) {
        if (operationCollection == null || operationCollection.isEmpty()) {
            return Empty.byteBuffer;
        } else {

            // 这个操作集合需要预留的空间大小
            ByteBuffer byteBuffer = ByteBuffer.allocate(OperationSet.messageSetSize(operationCollection));
            for (Operation operation : operationCollection) {
                writeMessage(byteBuffer, operation, offsetAssigner.nextAbsoluteOffset());
            }
            byteBuffer.rewind();
            return byteBuffer;
        }
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
     * 进行 offset 的分配，验证与更新 crc32 等信息
     */
    public ByteBufferOperationSet validateMessagesAndAssignOffsets(OffsetAssigner offsetAssigner) {
        int messagePosition = 0;
        byteBuffer.mark();

        while (messagePosition < sizeInBytes() - OperationSet.LogOverhead) {
            byteBuffer.position(messagePosition);
            byteBuffer.putLong(offsetAssigner.nextAbsoluteOffset());
            int messageSize = byteBuffer.getInt();
            ByteBuffer messageBufferShared = byteBuffer.slice();// The content of the new buffer will start at this buffer's current position.
            messageBufferShared.limit(messageSize);

            Operation operation = new Operation(messageBufferShared);
        }
    }

    /**
     * Returns this buffer's limit.
     */
    public int sizeInBytes() {
        return byteBuffer.limit();
    }

    @Override
    public int writeTo(GatheringByteChannel channel, long offset, int maxSize) throws IOException {
        return 0;
    }

    @Override
    public Iterator<OperationAndOffset> iterator() {
        return null;
    }

    /**
     * 将一个 operation，写入 buffer 中，并为其分配 offset
     */
    public static void writeMessage(ByteBuffer buffer, Operation operation, long offset) {
        buffer.putLong(offset);
        buffer.putInt(operation.size());
        buffer.put(operation.getByteBuffer());
        operation.getByteBuffer()
                 .rewind();
    }

    public static class OffsetAssigner {

        private AtomicLong index;

        private long start;

        public static OffsetAssigner create(long start) {
            OffsetAssigner offsetAssigner = new OffsetAssigner();
            offsetAssigner.start = start;
            offsetAssigner.index = new AtomicLong(start);
            return offsetAssigner;
        }

        private OffsetAssigner() {
        }

        public long nextAbsoluteOffset() {
            return index.incrementAndGet();
        }

        public long toInnerOffset(long offset) {
            return offset - start;
        }
    }
}


