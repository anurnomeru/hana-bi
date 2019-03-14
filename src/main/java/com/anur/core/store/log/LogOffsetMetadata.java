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
package com.anur.core.store.log;

import com.anur.exception.HanabiException;

/**
 * Created by Anur IjuoKaruKas on 2019/2/28
 *
 * A log offset structure, including:
 * 1. the message offset
 * 2. the base message offset of the located segment
 * 3. the physical position on the located segment
 */
public class LogOffsetMetadata {

    private static LogOffsetMetadata UnknownOffsetMetadata = new LogOffsetMetadata(-1, 0, 0);

    private static long UnknownSegBaseOffset = -1L;

    private static int UnknownFilePosition = -1;

    /** 这条操作记录的绝对位置 */
    private long messageOffset;

    /** LogSegment的基础offset */
    private long segmentBaseOffset;

    /** 相对位置 */
    private int relativePositionInSegment;

    public LogOffsetMetadata(long messageOffset, long segmentBaseOffset, int relativePositionInSegment) {
        this.messageOffset = messageOffset;
        this.segmentBaseOffset = segmentBaseOffset;
        this.relativePositionInSegment = relativePositionInSegment;
    }

    // check if this offset is already on an older segment compared with the given offset
    public boolean onOlderSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly()) {
            throw new HanabiException("$this cannot compare its segment info with $that since it only has message offset info");
        }

        return this.segmentBaseOffset < that.segmentBaseOffset;
    }

    // check if this offset is on the same segment with the given offset
    public boolean onSameSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly()) {
            throw new HanabiException("$this cannot compare its segment info with $that since it only has message offset info");
        }

        return this.segmentBaseOffset == that.segmentBaseOffset;
    }

    // compute the number of messages between this offset to the given offset
    public long offsetDiff(LogOffsetMetadata that) {
        return this.messageOffset - that.messageOffset;
    }

    // compute the number of bytes between this offset to the given offset
    // if they are on the same segment and this offset precedes the given offset
    public int positionDiff(LogOffsetMetadata that) {
        if (!onSameSegment(that)) {
            throw new HanabiException("$this cannot compare its segment position with $that since they are not on the same segment");
        }
        if (messageOffsetOnly()) {
            throw new HanabiException("$this cannot compare its segment position with $that since it only has message offset info");
        }

        return this.relativePositionInSegment - that.relativePositionInSegment;
    }

    public long getMessageOffset() {
        return messageOffset;
    }

    // decide if the offset metadata only contains message offset info
    private boolean messageOffsetOnly() {
        return segmentBaseOffset == LogOffsetMetadata.UnknownSegBaseOffset && relativePositionInSegment == LogOffsetMetadata.UnknownFilePosition;
    }
}
