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
        FetchDataInfo after = LogManager.INSTANCE.getAfter(new GenerationAndOffset(1, 1));

        FetchResponse fetchResponse = new FetchResponse(after);

        int start = fetchResponse.getFileOperationSet()
                                 .getStart();
        int end = fetchResponse.getFileOperationSet()
                               .getEnd();
        int count = end - start;

        ByteBuffer byteBuffer = ByteBuffer.allocate(count);
        FileIOUtil.openChannel(fetchResponse.getFileOperationSet()
                                            .getFile(), false)
                  .read(byteBuffer);
        byteBuffer.flip();

        ByteBufferOperationSet byteBufferOperationSet = new ByteBufferOperationSet(byteBuffer);

        Iterator<OperationAndOffset> iterator = byteBufferOperationSet.iterator();

        while (iterator.hasNext()) {
            OperationAndOffset next = iterator.next();
            System.out.println(
                next.getOffset());
            System.out.println(next
                .getOperation());
        }

        System.out.println();
    }
}
