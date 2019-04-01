package com.anur.io.store;

import java.io.File;
import java.util.Iterator;
import com.anur.core.command.modle.Operation;
import com.anur.core.command.common.OperationTypeEnum;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.io.store.common.OperationAndOffset;
import com.anur.io.store.log.LogManager;
import com.anur.io.store.operationset.ByteBufferOperationSet;
import com.anur.io.store.prelog.ByteBufPreLog;
import com.anur.io.store.prelog.ByteBufPreLogManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

/**
 * Created by Anur IjuoKaruKas on 2019/3/22
 */
public class TestByteBufPreLog {

    public static void main(String[] args) {
        LogManager.getINSTANCE();

        ByteBufPreLogManager byteBufPreLogManager = ByteBufPreLogManager.getINSTANCE();

        for (int i = 0; i < 100; i++) {
            byteBufPreLogManager
                .append(0, new ByteBufferOperationSet(
                    new Operation(OperationTypeEnum.REGISTER, "", ""), i));
        }

        byteBufPreLogManager.commit(new GenerationAndOffset(0, 10));
        byteBufPreLogManager.commit(new GenerationAndOffset(0, 10));
        byteBufPreLogManager.commit(new GenerationAndOffset(0, 12));
        byteBufPreLogManager.commit(new GenerationAndOffset(0, 12));
        byteBufPreLogManager.commit(new GenerationAndOffset(0, 10));
        byteBufPreLogManager.commit(new GenerationAndOffset(0, 10));
        byteBufPreLogManager.commit(new GenerationAndOffset(0, 100));
        byteBufPreLogManager.commit(new GenerationAndOffset(0, 100));

    }
}
