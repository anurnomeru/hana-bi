package com.anur.core.coordinate.model;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Consumer;
import com.anur.core.lock.ReentrantReadWriteLocker;
import com.anur.core.util.HanabiExecutors;
import com.anur.timewheel.TimedTask;

/**
 * Created by Anur IjuoKaruKas on 2019/3/27
 *
 * 消息发送处理器
 */
public class RequestProcessor extends ReentrantReadWriteLocker {

    public static RequestProcessor REQUIRE_NESS = null;

    /**
     * 是否已经完成了这个请求过程（包括接收response）
     */
    private volatile boolean complete = false;

    /**
     * 如何消费回调
     */
    private Consumer<ByteBuffer> callBack;

    /**
     * 在完全收到消息回调后做什么，用于重启定时任务
     */
    private Runnable afterCompleteReceive;

    /**
     * 重新请求的定时任务
     */
    private TimedTask timedTask;

    public RequestProcessor(Consumer<ByteBuffer> callBack, Runnable afterCompleteReceive) {
        this.callBack = callBack;
        this.afterCompleteReceive = afterCompleteReceive;
    }

    public boolean isComplete() {
        return readLockSupplierCompel(() -> complete);
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
            if (!complete) {
                complete = true;
                Optional.ofNullable(timedTask)
                        .ifPresent(TimedTask::cancel);
                Optional.ofNullable(callBack)
                        .ifPresent(cb -> HanabiExecutors.Companion.execute(() -> cb.accept(byteBuffer)));
                Optional.ofNullable(afterCompleteReceive)
                        .ifPresent(HanabiExecutors.Companion::execute);
            }
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
    public boolean registerTask(TimedTask timedTask) {
        return readLockSupplierCompel(() -> {
            if (!complete) {
                this.timedTask = timedTask;
            }
            return !complete;
        });
    }
}
