package com.anur.core.coordinate.model;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.core.lock.ReentrantReadWriteLocker;
import com.anur.core.util.HanabiExecutors;
import com.anur.timewheel.TimedTask;

/**
 * Created by Anur IjuoKaruKas on 2019/3/27
 *
 * 消息发送处理器
 */
public class RequestProcessor extends ReentrantReadWriteLocker {

    public static RequestProcessor REQUIRE_NESS() {
        return new RequestProcessor(byteBuffer -> {
        });
    }

    private Logger logger = LoggerFactory.getLogger(RequestProcessor.class);

    /**
     * 是否已经完成了这个请求过程（包括接收response）
     */
    private volatile boolean complete;

    /**
     * 如何消费回调
     */
    private Consumer<ByteBuffer> callBack;

    /**
     * 重新请求的定时任务
     */
    private TimedTask timedTask;

    public RequestProcessor(Consumer<ByteBuffer> callBack) {
        this.callBack = callBack;
    }

    public boolean isComplete() {
        return readLockSupplier(() -> complete);
    }

    /**
     * 已经完成了此任务
     */
    public void complete() {
        writeLockSupplier(() -> {
            complete = true;
            Optional.ofNullable(timedTask)
                    .ifPresent(TimedTask::cancel);
            return null;
        });
    }

    /**
     * 已经完成了此任务
     */
    public void complete(ByteBuffer byteBuffer) {
        writeLockSupplier(() -> {
            complete = true;
            Optional.ofNullable(timedTask)
                    .ifPresent(TimedTask::cancel);
            HanabiExecutors.submit(() -> {
                callBack.accept(byteBuffer);
            });
            return null;
        });
    }

    /**
     * 取消此任务
     */
    public void cancel() {
        writeLockSupplier(() -> {
            complete = true;
            Optional.ofNullable(timedTask)
                    .ifPresent(TimedTask::cancel);
            return null;
        });
    }

    /**
     * 向此回调
     */
    public void registerTask(TimedTask timedTask) {
        readLockSupplier(() -> {
            if (!complete) {
                this.timedTask = timedTask;
            }
            return null;
        });
    }
}
