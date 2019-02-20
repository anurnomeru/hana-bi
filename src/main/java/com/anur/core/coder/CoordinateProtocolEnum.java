package com.anur.core.coder;

import com.anur.core.coordinate.model.HashGet;
import com.anur.core.coordinate.model.HashSetNx;

/**
 * Created by Anur IjuoKaruKas on 2/19/2019
 */
public enum CoordinateProtocolEnum {

    HASH_GET(HashGet.class),
    HASH_SET_NX(HashSetNx.class);

    CoordinateProtocolEnum(Class clazz) {
        this.clazz = clazz;
    }

    public Class<?> clazz;
}
