package com.anur.timewheel;

import java.util.Date;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Created by Anur IjuoKaruKas on 2018/10/16
 *
 * 槽、或者说桶
 */
public class Bucket implements Delayed {

    /** 当前槽的过期时间 */
    private AtomicLong expiration = new AtomicLong(-1L);

    /** 根节点 */
    private TimedTask root = new TimedTask(-1L, null);

    {
        root.pre = root;
        root.next = root;
    }

    /**
     * 设置某个槽的过期时间
     */
    public boolean setExpire(long expire) {
        return expiration.getAndSet(expire) != expire;
    }

    /**
     * 获取某个槽的过期时间
     */
    public long getExpire() {
        return expiration.get();
    }

    /**
     * 新增任务到bucket
     */
    public void addTask(TimedTask timedTask) {
        synchronized (timedTask) {
            if (timedTask.bucket == null) {
                timedTask.bucket = this;
                TimedTask tail = root.pre;

                timedTask.next = root;
                timedTask.pre = tail;

                tail.next = timedTask;
                root.pre = timedTask;
            }
        }
    }

    /**
     * 从bucket移除任务
     */
    public void removeTask(TimedTask timedTask) {
        synchronized (timedTask) {
            if (timedTask.bucket.equals(this)) {// 这里可能有bug
                timedTask.next.pre = timedTask.pre;
                timedTask.pre.next = timedTask.next;
                timedTask.bucket = null;
                timedTask.next = null;
                timedTask.pre = null;
            }
        }
    }

    /**
     * 重新分配槽
     */
    public synchronized void flush(Consumer<TimedTask> flush) {
        TimedTask timedTask = root.next;// 从尾巴开始（最先加进去的）

        while (!timedTask.equals(root)) {
            this.removeTask(timedTask);
            flush.accept(timedTask);
            timedTask = root.next;
        }
        expiration.set(-1L);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return Math.max(0, unit.convert(expiration.get() - System.currentTimeMillis(), TimeUnit.MILLISECONDS));
    }

    @Override
    public int compareTo(Delayed o) {
        if (o instanceof Bucket) {
            return Long.compare(expiration.get(), ((Bucket) o).expiration.get());
        }
        return 0;
    }

    @Override
    public String toString() {
        return "Bucket{" +
            "过期时间=" + new Date(expiration.get()).toString() +
            ", 延迟=" + this.getDelay(TimeUnit.SECONDS) + "s" +
            '}';
    }
}
