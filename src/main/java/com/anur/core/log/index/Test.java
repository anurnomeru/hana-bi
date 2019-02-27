package com.anur.core.log.index;

/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 */
public class Test {

    private int mmap;

    private int entries;

    public Test(int mmap) {
        this.entries = mmap / 8;
    }

    public int Entries() { // 等同于 def maxEntries: Int = _maxEntries
        return entries;
    }

}
