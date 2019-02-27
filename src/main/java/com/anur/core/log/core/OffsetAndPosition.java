package com.anur.core.log.core;


/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 */
public class OffsetAndPosition {

    /**
     * 代表逻辑上的文件offset
     */
    public long offset;

    /**
     * 代表物理上的位置
     */
    public int position;

    public OffsetAndPosition(long offset, int position) {
        this.offset = offset;
        this.position = position;
    }
}
