package com.anur.core.store.common;

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 *
 * 记录了一条操作记录的 逻辑上的 offset 信息，与物理上的 position 信息
 */
public class OffsetAndPosition {

    /**
     * offset 代表逻辑上的文件位置
     */
    private long offset;

    /**
     * position 代表物理上的位置
     */
    private int position;

    public OffsetAndPosition(long offset, int position) {
        this.offset = offset;
        this.position = position;
    }

    public long getOffset() {
        return offset;
    }

    public int getPosition() {
        return position;
    }
}
