package com.anur.core.coder;

import com.anur.core.coordinate.model.HashSetNx;

/**
 * Created by Anur IjuoKaruKas on 2/20/2019
 */
public class TestCoordinateCoder {

    public static void main(String[] args) {
        System.out.println(CoordinateCoder.encode(CoordinateProtocolEnum.HASH_SET_NX, "asdfasdfasdfasdfasdfasdfasdf", new HashSetNx("test", true, "asdf")));
    }
}
