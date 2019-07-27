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
 *
 * 测试能否正确写入预日志，并进行提交
 */
public class TestByteBufPreLog {

    public static void main(String[] args) throws InterruptedException {
        new Operation(OperationTypeEnum.SETNX, "1", "");

        LogManager instance = LogManager.INSTANCE;

        ByteBufPreLogManager byteBufPreLogManager = ByteBufPreLogManager.INSTANCE;

        HanabiExecutors.Companion.submit(() -> {
            while (true) {
                Thread.sleep(100);
                byteBufPreLogManager.commit(new GenerationAndOffset(0, 30000000));
            }
        });

        HanabiExecutors.Companion.execute(() -> {
            long start = System.currentTimeMillis();
            for (int i = 1000; i < 2000; i++) {
                byteBufPreLogManager
                    .append(0, new ByteBufferOperationSet(
                        new Operation(OperationTypeEnum.SETNX, "Asssssssss",
                            "YYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSB"), i));
            }
        });

        byteBufPreLogManager.commit(new GenerationAndOffset(0, 30000000));

        Thread.sleep(100000);
    }
}
