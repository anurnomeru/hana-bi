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
        offsetManager.cover(new GenerationAndOffset(10, 5));
        System.out.println(offsetManager.load());
        System.out.println(offsetManager.load());
        offsetManager.cover(new GenerationAndOffset(10, 6));
        System.out.println(offsetManager.load());
        System.out.println(offsetManager.load());
    }
}
