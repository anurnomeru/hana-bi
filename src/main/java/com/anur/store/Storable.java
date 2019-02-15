package com.anur.store;


/**
 * Created by Anur IjuoKaruKas on 2/15/2019
 */
public interface Storable {

    String get(String key);

    String set(String key, String val, boolean nx, long expireMs);
}
