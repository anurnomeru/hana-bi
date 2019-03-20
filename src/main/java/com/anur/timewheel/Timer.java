package com.anur.timewheel;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Created by Anur IjuoKaruKas on 2018/10/17
 */
public class Timer {

    /** 最底层的那个时间轮 */
    private TimeWheel timeWheel;

    /** 对于一个Timer以及附属的时间轮，都只有一个delayQueue */
    private DelayQueue<Bucket> delayQueue = new DelayQueue<>();

    /** 只有一个线程的无限阻塞队列线程池 */
    private ExecutorService workerThreadPool;

    private ExecutorService bossThreadPool;

    private static Timer INSTANCE;

    public static Timer getInstance() {
        if (INSTANCE == null) {
            synchronized (Timer.class) {
                if (INSTANCE == null) {
                    INSTANCE = new Timer();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * 新建一个Timer，同时新建一个时间轮
     */
    private Timer() {
        workerThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactoryBuilder().setPriority(10)
                                                                                       .setNameFormat("TimeWheelWorker")
                                                                                       .build());
        bossThreadPool = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setPriority(10)
                                                                                   .setNameFormat("TimeWheelBoss")
                                                                                   .build());
        timeWheel = new TimeWheel(20, 10, System.currentTimeMillis(), delayQueue);

        bossThreadPool.execute(() -> {
            while (true) {
                INSTANCE.advanceClock(20);
            }
        });
    }

    /**
     * 将任务添加到时间轮
     */
    public void addTask(TimedTask timedTask) {
        if (!timeWheel.addTask(timedTask)) {
            if (!timedTask.isCancel()) {
                workerThreadPool.submit(timedTask.getTask());
            }
        }
    }

    /**
     * 推进一下时间轮的指针，并且将delayQueue中的任务取出来再重新扔进去
     */
    private void advanceClock(long timeout) {
        try {
            Bucket bucket = delayQueue.poll(timeout, TimeUnit.MILLISECONDS);
            if (bucket != null) {
                timeWheel.advanceClock(bucket.getExpire());
                bucket.flush(this::addTask);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
