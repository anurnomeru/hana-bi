package com.anur.io.operationset;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.BiConsumer;
import com.anur.core.coordinate.CoordinateClientOperator;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.struct.coordinate.FetchResponse;
import com.anur.io.core.coder.CoordinateDecoder;
import com.anur.io.core.handle.ByteBufferMsgConsumerHandler;
import com.anur.io.core.handle.ErrorHandler;
import com.anur.io.store.common.FetchDataInfo;
import com.anur.io.store.common.OperationAndOffset;
import com.anur.io.store.log.LogManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * Created by Anur IjuoKaruKas on 2019/4/2
 *
 * 测试从 leader fetch 消息
 */
public class TestFileOperationSet {

    public static void main(String[] args) throws IOException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        Field field = CoordinateClientOperator.class.getDeclaredField("CLIENT_MSG_CONSUMER");
        field.setAccessible(true);
        EmbeddedChannel channel = new EmbeddedChannel(new CoordinateDecoder(), new ByteBufferMsgConsumerHandler(
            (BiConsumer<ChannelHandlerContext, ByteBuffer>) field.get(CoordinateClientOperator.class)), new ErrorHandler());

        LogManager logManager = LogManager.getINSTANCE();
        FetchDataInfo fetchDataInfo = logManager.getAfter(new GenerationAndOffset(1, 29998888));

        FetchResponse fetchResponse = new FetchResponse(fetchDataInfo);

        FileRegion fileRegion = new DefaultFileRegion(fetchDataInfo.getFileOperationSet()
                                                                   .getFileChannel(), fetchDataInfo.getFileOperationSet()
                                                                                                   .getStart(), fetchDataInfo.getFileOperationSet()
                                                                                                                             .getEnd());
        Iterator<OperationAndOffset> iterator = fetchDataInfo.getFileOperationSet()
                                                             .iterator();

        while (iterator.hasNext()) {
            OperationAndOffset next = iterator.next();
            next.getOffset();
        }

        ByteBuf size = Unpooled.buffer(4);
        size.writeInt(fetchResponse.totalSize());

        channel.writeOutbound(fileRegion);
        Object out = channel.readOutbound();

        channel.writeInbound(Unpooled.wrappedBuffer(size));
        channel.writeInbound(Unpooled.wrappedBuffer(fetchResponse.getByteBuffer()));
        channel.writeInbound(out);
        channel.finish();

        System.out.println(channel.writeInbound(size, fileRegion));
        Thread.sleep(10000);
    }
}
