package com.anur.core.elect;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Created by Anur IjuoKaruKas on 2019/1/22
 *
 * 投票箱
 */
public class VotesBox {

    private Map<String/* serverName */, Boolean/* active */> box;

    private ReentrantLock reentrantLock;

    /**
     * 该投票箱的世代信息
     */
    private int generation;

    public VotesBox() {
        reentrantLock = new ReentrantLock();
        box = new HashMap<>();
    }

    /**
     * 投票
     */
    public int vote(Votes votes) {
        return this.lockSupplier(() -> {
            if (votes.getGeneration() > this.generation) {// 如果有选票的世代已经大于当前世代，那么重置投票箱
                this.initVoteBox(this.generation);
            }

            box.put(votes.getServerName(), votes.isActive());
            return this.generation;
        });
    }

    /**
     * 初始化投票箱
     */
    public void initVoteBox(int genneration) {
        this.lockSupplier(() -> {
            this.generation = genneration;
            box = new HashMap<>();
            return null;
        });
    }

    /**
     * 提供一个统一的锁入口
     */
    private <T> T lockSupplier(Supplier<T> supplier) {
        T t;
        try {
            reentrantLock.lock();
            t = supplier.get();
        } finally {
            reentrantLock.unlock();
        }
        return t;
    }
}
