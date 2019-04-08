package com.anur.io.store;

import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.Operation;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.util.HanabiExecutors;
import com.anur.io.store.log.LogManager;
import com.anur.io.store.operationset.ByteBufferOperationSet;
import com.anur.io.store.prelog.ByteBufPreLogManager;

/**
 * Created by Anur IjuoKaruKas on 2019/3/22
 */
public class TestByteBufPreLog {

    public static void main(String[] args) throws InterruptedException {
        new Operation(OperationTypeEnum.SETNX, "1", "");

        LogManager.getINSTANCE();

        ByteBufPreLogManager byteBufPreLogManager = ByteBufPreLogManager.getINSTANCE();

        HanabiExecutors.excute(() -> {
            long start = System.currentTimeMillis();
            for (int i = 30000000; i < 30000999; i++) {
                byteBufPreLogManager
                    .append(0, new ByteBufferOperationSet(
                        new Operation(OperationTypeEnum.SETNX, "Asssssssss", "YYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSB"), i));
            }
            System.out.println(start - System.currentTimeMillis());
        });

        HanabiExecutors.submit(() -> {
            while (true) {
                Thread.sleep(100);
                byteBufPreLogManager.commit(new GenerationAndOffset(0, 30000000));
            }
        });

        Thread.sleep(100000);
    }
}
