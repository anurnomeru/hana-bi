package com.anur.engine.api.common

import com.anur.engine.api.common.base.EngineRequest
import com.anur.engine.api.common.base.EngineResponse


/**
 * Created by Anur IjuoKaruKas on 2019/9/25
 */
interface ApiDispatcher {
    fun api(byte: Byte): (EngineRequest) -> EngineResponse
}