package com.anur.core.struct.coordinate

import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.AbstractTimedStruct
import io.netty.buffer.Unpooled
import io.netty.channel.Channel

/**
 * Created by Anur IjuoKaruKas on 2019/7/11
 */
class RecoveryComplete : AbstractTimedStruct() {

    init {
        init(OriginMessageOverhead, OperationTypeEnum.RECOVERY_RESPONSE) { }
    }

    override fun writeIntoChannel(channel: Channel?) {
        channel!!.write(Unpooled.wrappedBuffer(buffer))
    }

    override fun totalSize(): Int {
        return size()
    }
}