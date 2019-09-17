package com.anur.io.hanalog;

import java.nio.charset.Charset;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.Operation;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.util.HanabiExecutors;
import com.anur.io.hanalog.log.LogManager;
import com.anur.io.hanalog.operationset.ByteBufferOperationSet;
import com.anur.io.hanalog.prelog.ByteBufPreLogManager;

/**
 * Created by Anur IjuoKaruKas on 2019/3/22
 *
 * 测试能否正确写入预日志，并进行提交
 */
public class TestByteBufPreLog {

    public static void main(String[] args) throws InterruptedException {
        new Operation(OperationTypeEnum.SETNX, "1", "".getBytes(Charset.defaultCharset()));

        LogManager instance = LogManager.INSTANCE;
        ByteBufPreLogManager byteBufPreLogManager = ByteBufPreLogManager.INSTANCE;
        HanabiExecutors.INSTANCE.submit(() -> {
            while (true) {
                Thread.sleep(100);
                byteBufPreLogManager.commit(new GenerationAndOffset(0, 30000000));
            }
        });

        HanabiExecutors.INSTANCE.execute(() -> {
            for (int i = 1000; i < 2000; i++) {
                byteBufPreLogManager
                    .append(0, new ByteBufferOperationSet(
                        new Operation(OperationTypeEnum.SETNX, "Asssssssss",
                            "YYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSBYYSSB".getBytes(Charset.defaultCharset())), i));
            }
        });

        byteBufPreLogManager.commit(new GenerationAndOffset(0, 30000000));
        Thread.sleep(100000);
    }
}
