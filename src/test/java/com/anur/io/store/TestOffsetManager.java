package com.anur.io.store;

import com.anur.core.elect.model.GenerationAndOffset;

/**
 * Created by Anur IjuoKaruKas on 2019/3/26
 */
public class TestOffsetManager {

    public static void main(String[] args) {
        OffsetManager offsetManager = OffsetManager.getINSTANCE();
        System.out.println(offsetManager.load());
        System.out.println(offsetManager.load());
        offsetManager.cover(new GenerationAndOffset(1231233123123L, 123123123123L));
        System.out.println(offsetManager.load());
        System.out.println(offsetManager.load());
        offsetManager.cover(new GenerationAndOffset(9, 1));
        System.out.println(offsetManager.load());
        System.out.println(offsetManager.load());
    }
}
