package com.anur.engine.api.common

import com.anur.engine.api.common.api.StrApiConst
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
//                    println("开始持久化事务id为 ${req.trxId}, 键为${req.key}, 内容为 ${req.value} 的操作")
                    EngineResponse(1, "啦啦啦啦")
                }
            }
            else -> {
                throw UnSupportedApiException()
            }
        }
    }


}