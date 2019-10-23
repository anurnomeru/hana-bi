package com.anur.engine.trx.manager


/**
 * Created by Anur IjuoKaruKas on 2019/10/16
 */
enum class TrxSegmentEnum(val sign: Byte) {
    UN_COMMIT(0),
    COMMITTED(1);
}