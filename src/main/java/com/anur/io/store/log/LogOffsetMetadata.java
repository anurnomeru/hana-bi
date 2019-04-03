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
package com.anur.io.store.log;

/**
 * Created by Anur IjuoKaruKas on 2019/2/28
 *
 * A log offset structure, including:
 * 1. the generation
 * 2. the message offset
 * 3. the base message offset of the located segment
 * 4. the physical position on the located segment
 */
public class LogOffsetMetadata {

    private static LogOffsetMetadata UnknownOffsetMetadata = new LogOffsetMetadata(-1, -1, 0, 0);

    /** 消息所属世代 */
    private long generation;

    /** 这条操作记录的绝对位置 */
    private long messageOffset;

    /** LogSegment的 Base offset */
    private long segmentBaseOffset;

    /** 相对位置 */
    private int relativePositionInSegment;

    public LogOffsetMetadata(long generation, long messageOffset, long segmentBaseOffset, int relativePositionInSegment) {
        this.generation = generation;
        this.messageOffset = messageOffset;
        this.segmentBaseOffset = segmentBaseOffset;
        this.relativePositionInSegment = relativePositionInSegment;
    }

    public long getGeneration() {
        return generation;
    }

    public long getMessageOffset() {
        return messageOffset;
    }

    public long getSegmentBaseOffset() {
        return segmentBaseOffset;
    }

    public int getRelativePositionInSegment() {
        return relativePositionInSegment;
    }
}
