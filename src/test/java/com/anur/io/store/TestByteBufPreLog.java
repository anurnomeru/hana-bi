package com.anur.io.store;

import com.anur.io.store.common.Operation;
import com.anur.io.store.common.OperationTypeEnum;
import com.anur.io.store.prelog.ByteBufPreLog;
import io.netty.buffer.ByteBuf;

/**
 * Created by Anur IjuoKaruKas on 2019/3/22
 */
public class TestByteBufPreLog {

    public static void main(String[] args) {
        ByteBufPreLog preLog = new ByteBufPreLog(10);

        for (int i = 0; i < 1000; i++) {
            preLog.append(new Operation(OperationTypeEnum.REGISTER, "", ""), i);
        }

        ByteBuf byteBuf1 = preLog.getAfter(995);

        ByteBuf byteBuf2 = preLog.getBefore(995);

        preLog.discardBefore(995);

        ByteBuf byteBuf3 = preLog.getBefore(995);
        System.out.println();
    }
}
