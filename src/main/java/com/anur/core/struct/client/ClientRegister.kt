package com.anur.core.struct.client

import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.coordinate.Register
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/12/15
 *
 * 客户端向协调器注册自己
 */
class ClientRegister : Register {

    constructor() : super("", OperationTypeEnum.CLIENT_REGISTER)

    constructor(byteBuffer: ByteBuffer?) : super(byteBuffer)
}