package com.anur.core.log;

/**
 * Created by Anur IjuoKaruKas on 2019/3/5
 */
public class LogAppendInfo {

    private long firstOffset;

    private long lastOffset;

    private int validBytes;

    public LogAppendInfo(long firstOffset, long lastOffset, int validBytes) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.validBytes = validBytes;
    }
}
