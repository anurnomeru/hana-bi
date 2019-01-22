package com.anur.timewheel;

import java.util.concurrent.DelayQueue;

/**
 * Created by Anur IjuoKaruKas on 2018/10/16
 *
 * 时间轮，可以推进时间和添加任务
 */
public class TimeWheel {

    /** 一个时间槽的时间 */
    private long tickMs;

    /** 时间轮大小 */
    private int wheelSize;

    /** 时间跨度 */
    private long interval;

    /** 槽 */
    private Bucket[] buckets;

    /** 时间轮指针 */
    private long currentTimestamp;

    /** 上层时间轮 */
    private volatile TimeWheel overflowWheel;

    /** 对于一个Timer以及附属的时间轮，都只有一个delayQueue */
    private DelayQueue<Bucket> delayQueue;

    public TimeWheel(long tickMs, int wheelSize, long currentTimestamp, DelayQueue<Bucket> delayQueue) {
        this.currentTimestamp = currentTimestamp;
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.interval = tickMs * wheelSize;
        this.buckets = new Bucket[wheelSize];
        this.currentTimestamp = currentTimestamp - (currentTimestamp % tickMs);
        this.delayQueue = delayQueue;

        for (int i = 0; i < wheelSize; i++) {
            buckets[i] = new Bucket();
        }
    }

    private TimeWheel getOverflowWheel() {
        if (overflowWheel == null) {
            synchronized (this) {
                if (overflowWheel == null) {
                    overflowWheel = new TimeWheel(interval, wheelSize, currentTimestamp, delayQueue);
                }
            }
        }
        return overflowWheel;
    }

    /**
     * 添加任务到某个时间轮
     */
    public boolean addTask(TimedTask timedTask) {
        long expireTimestamp = timedTask.getExpireTimestamp();
        long delayMs = expireTimestamp - currentTimestamp;
        if (delayMs < tickMs) {// 到期了
            return false;
        } else {

            // 扔进当前时间轮的某个槽中，只有时间【大于某个槽】，才会放进去
            if (delayMs < interval) {
                int bucketIndex = (int) (((delayMs + currentTimestamp) / tickMs) % wheelSize);

                Bucket bucket = buckets[bucketIndex];
                bucket.addTask(timedTask);
                if (bucket.setExpire(delayMs + currentTimestamp - (delayMs + currentTimestamp) % tickMs)) {
                    delayQueue.offer(bucket);
                }
            } else {
                TimeWheel timeWheel = getOverflowWheel();// 当maybeInThisBucket大于等于wheelSize时，需要将它扔到上一层的时间轮
                timeWheel.addTask(timedTask);
            }
        }
        return true;
    }

    /**
     * 尝试推进一下指针
     */
    public void advanceClock(long timestamp) {
        if (timestamp >= currentTimestamp + tickMs) {
            currentTimestamp = timestamp - (timestamp % tickMs);

            if (overflowWheel != null) {
                this.getOverflowWheel()
                    .advanceClock(timestamp);
            }
        }
    }
}
