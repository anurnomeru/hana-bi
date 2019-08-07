package com.anur.core.coordinate.model

import com.anur.core.struct.OperationTypeEnum
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/8/7
 */
class CoordinateRequest(val msg: ByteBuffer, val typeEnum: OperationTypeEnum, val channel: Channel?) {
    companion object {
        val AllDone = CoordinateRequest(ByteBuffer.allocate(0), OperationTypeEnum.NONE, null)
    }
}