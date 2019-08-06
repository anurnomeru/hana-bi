package com.anur.io.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.struct.coordinate.FetchResponse;
import com.anur.core.util.FileIOUtil;
import com.anur.io.store.common.FetchDataInfo;
import com.anur.io.store.common.OperationAndOffset;
import com.anur.io.store.log.LogManager;
import com.anur.io.store.operationset.ByteBufferOperationSet;

/**
 * Created by Anur IjuoKaruKas on 2019/7/29
 *
 * 测试日志获取是否正常
 */
public class TestFetchLog {

    public static void main(String[] args) throws IOException {
        FetchDataInfo after = LogManager.INSTANCE.getAfter(new GenerationAndOffset(4, 1000000));

        FetchResponse fetchResponse = new FetchResponse(after);

        long generation = fetchResponse.getGeneration();
        System.out.println("gen: " + generation);

        int start = fetchResponse.getFileOperationSet()
                                 .getStart();
        int end = fetchResponse.getFileOperationSet()
                               .getEnd();
        int count = end - start;

        ByteBuffer byteBuffer = ByteBuffer.allocate(count);
        FileIOUtil.openChannel(fetchResponse.getFileOperationSet()
                                            .getFile(), false)
                  .position(start)
                  .read(byteBuffer);
        byteBuffer.flip();

        ByteBufferOperationSet byteBufferOperationSet = new ByteBufferOperationSet(byteBuffer);
        OperationAndOffset last = null;

        Iterator<OperationAndOffset> iterator = byteBufferOperationSet.iterator();
        while (iterator.hasNext()) {
            OperationAndOffset next = iterator.next();
            System.out.println(next.getOffset());
            last = next;
        }

        System.out.println("off " + last.getOffset());

        System.out.println();
    }
}
