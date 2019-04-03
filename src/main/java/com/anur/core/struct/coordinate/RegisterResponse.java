package com.anur.core.struct.coordinate;

import java.nio.ByteBuffer;
import com.anur.core.struct.OperationTypeEnum;
/**
 * Created by Anur IjuoKaruKas on 4/3/2019
 *
 * 注册回包
 */
public class RegisterResponse extends Register {

    public RegisterResponse(String serverName) {
        super(serverName);
        buffer.mark();
        buffer.position(TypeOffset);
        buffer.putInt(OperationTypeEnum.REGISTER_RESPONSE.byteSign);
        buffer.reset();
    }

    public RegisterResponse(ByteBuffer byteBuffer) {
        super(byteBuffer);
    }
}
