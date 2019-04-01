package com.anur.io.store;

import java.io.File;
import java.util.Iterator;
import com.anur.core.command.modle.Operation;
import com.anur.core.command.common.OperationTypeEnum;
import com.anur.io.store.common.OperationAndOffset;
import com.anur.io.store.operationset.ByteBufferOperationSet;
import com.anur.io.store.prelog.ByteBufPreLog;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

/**
 * Created by Anur IjuoKaruKas on 2019/3/22
 */
public class TestByteBufPreLog {

    public static void main(String[] args) {
        //        ByteBufPreLog preLog = new ByteBufPreLog(10);
        //
        //        for (int i = 0; i < 1000; i++) {
        //            preLog.append(new Operation(OperationTypeEnum.REGISTER, "123", "444"), i);
        //        }
        //
        //        ByteBuf byteBuf1 = preLog.getAfter(995);
        //
        //        ByteBuf byteBuf2 = preLog.getBefore(995);
        //
        //        preLog.discardBefore(995);
        //
        //        ByteBuf byteBuf3 = preLog.getBefore(995);
        //        System.out.println();
        //
        //        Iterator<OperationAndOffset> iterator = new ByteBufferOperationSet(byteBuf2.nioBuffer()).iterator();
        //
        //        while (iterator.hasNext()) {
        //            System.out.println(iterator.next()
        //                                       .getOffset());
        //        }

        ByteBufPreLog preLog = new ByteBufPreLog(10);

        preLog.append(new Operation(OperationTypeEnum.REGISTER, "123", "444"), 1);

        CompositeByteBuf byteBuf2 = preLog.getBefore(2);

        Iterator<OperationAndOffset> iterator = new ByteBufferOperationSet(byteBuf2.nioBuffer()).iterator();

        while (iterator.hasNext()) {
            System.out.println(iterator.next()
                                       .getOffset());
        }
    }
}
