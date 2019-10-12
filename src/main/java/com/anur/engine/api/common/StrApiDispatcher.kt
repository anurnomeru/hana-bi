package com.anur.engine.api.common

import com.anur.engine.api.common.constant.str.StrApiConst
import com.anur.engine.api.common.base.EngineRequest
import com.anur.engine.api.common.base.EngineResponse
import com.anur.exception.UnSupportedApiException


/**
 * Created by Anur IjuoKaruKas on 2019/9/25
 */
object StrApiDispatcher : ApiDispatcher {

    override fun api(byte: Byte): (EngineRequest) -> EngineResponse {
        return when (byte) {
            StrApiConst.insert -> {
                return { req ->
                    EngineResponse(1, "啦啦啦啦")
                }
            }
            else -> {
                throw UnSupportedApiException()
            }
        }
    }


}