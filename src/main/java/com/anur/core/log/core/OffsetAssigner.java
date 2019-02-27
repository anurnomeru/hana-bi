package com.anur.core.log.core;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 *
 * 其实就是一个原子 Long，递增进行分配
 */
public class OffsetAssigner {

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
