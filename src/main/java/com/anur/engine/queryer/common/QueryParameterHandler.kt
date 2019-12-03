package com.anur.engine.queryer.common

import com.anur.engine.trx.watermark.WaterMarker

/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 */
class QueryParameterHandler(val trxId: Long, val key: String, val waterMarker: WaterMarker)