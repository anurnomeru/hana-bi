package com.anur.io.operationset;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import com.anur.core.coordinate.CoordinateClientOperator;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.struct.coordinate.FetchResponse;
import com.anur.core.struct.coordinate.Register;
import com.anur.io.core.coder.CoordinateDecoder;
import com.anur.io.core.handle.ByteBufferMsgConsumerHandler;
import com.anur.io.core.handle.ErrorHandler;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * Created by Anur IjuoKaruKas on 2019/4/2
 *
 * 测试从 leader fetch 消息
 */
public class TestFileOperationSet {

    public static void main(String[] args) throws IOException, NoSuchFieldException, IllegalAccessException, InterruptedException {

        ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        Lock read = reentrantReadWriteLock.readLock();
        Lock write = reentrantReadWriteLock.writeLock();

        read.lock();
        read.lock();
        System.out.println();

        Field field = CoordinateClientOperator.class.getDeclaredField("CLIENT_MSG_CONSUMER");
        field.setAccessible(true);
        EmbeddedChannel channel = new EmbeddedChannel(new CoordinateDecoder(), new ByteBufferMsgConsumerHandler(
            (BiConsumer<ChannelHandlerContext, ByteBuffer>) field.get(CoordinateClientOperator.class)), new ErrorHandler());

        Register register = new Register("123123");

        channel.writeInbound(Unpooled.copyInt(register.size()));
        channel.writeInbound(Unpooled.wrappedBuffer(register.getByteBuffer()));
        channel.finish();

        Thread.sleep(10000);
    }
}
