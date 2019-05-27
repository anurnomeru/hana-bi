package com.anur.core.util;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.anur.exception.HanabiException;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Created by Anur IjuoKaruKas on 2/1/2019
 */
public class HanabiExecutors {

    private static Logger logger = LoggerFactory.getLogger("HanabiExecutors");

    /**
     * 统一管理的线程池
     */
    private static ExecutorService Pool;

    /**
     * 线程池的 Queue
     */
    private static BlockingDeque<Runnable> MissionQueue;

    static {
        // 任务可无限堆积
        MissionQueue = new LinkedBlockingDeque<>();

        int coreCount = Runtime.getRuntime()
                               .availableProcessors();
        int threadCount = coreCount * 2;
        logger.info("创建 Hanabi 线程池 => 机器核心数为 {}, 故创建线程 {} 个", coreCount, threadCount);
        Pool = new _HanabiExecutors(threadCount, threadCount, 5, TimeUnit.MILLISECONDS, MissionQueue, new ThreadFactoryBuilder().setNameFormat("Hana Pool")
                                                                                                                                .setDaemon(true)
                                                                                                                                .build());
    }

    public static void execute(Runnable runnable) {
        Pool.execute(runnable);
    }

    public static <T> Future<T> submit(Callable<T> task) {
        return Pool.submit(task);
    }

    public static int getBlockSize() {
        return MissionQueue.size();
    }

    public static class _HanabiExecutors extends ThreadPoolExecutor {

        public _HanabiExecutors(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            if (t == null && r instanceof Future<?>) {
                try {
                    Future<?> future = (Future<?>) r;
                    if (future.isDone()) {
                        future.get();
                    }
                } catch (CancellationException ce) {
                    t = ce;
                } catch (ExecutionException ee) {
                    t = ee.getCause();
                } catch (InterruptedException ie) {
                    Thread.currentThread()
                          .interrupt();
                }
            }
            if (t != null) {
                logger.error(t.getMessage(), t);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> map = new ConcurrentHashMap<>();

        HanabiExecutors.multiJob(
            new MultiJober<>(() -> 4, System.out::println),
            new MultiJober<>(() -> {
                if ("业务处理" != "处理中") {
                    return "啦啦啦";
                }
                return "喵喵喵";
            }, System.out::println),
            new MultiJober<>(() -> {
                if (1 == 1) {
                    throw new HanabiException("测试任务执行报错");
                }
                return 999;
            }, i -> System.out.println(i + 5)),
            new MultiJober<>(() -> 1, i -> {
                throw new HanabiException("测试结果消费报错");
            }),
            new MultiJober<>(() -> 0, i -> map.put("list", i)),
            new MultiJober<>(() -> Lists.newArrayList("123", 4444, 677777, "zxcv"), l -> map.put("list1", l)),
            new MultiJober<>(() -> "what the hell", l -> map.put("list2", l))
        );

        System.out.println("==================");

        map.entrySet()
           .forEach(e -> System.out.println("k -> " + e.getKey() + " ,v -> " + e.getValue()));
    }

    public static void multiJob(MultiJober... multiJober) {
        CountDownLatch cdl = new CountDownLatch(multiJober.length);
        Arrays.stream(multiJober)
              .map(MultiJober::execute)
              .forEach(mj -> mj.consume(cdl));

        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static class MultiJober<T> {

        private Callable<T> callable;

        private Consumer<T> jobConsumer;

        private Future<T> future;

        public MultiJober(Callable<T> callable, Consumer<T> jobConsumer) {
            this.callable = callable;
            this.jobConsumer = jobConsumer;
        }

        protected MultiJober execute() {
            future = Pool.submit(callable);
            return this;
        }

        protected void consume(CountDownLatch cdl) {
            Pool.submit(() -> {
                try {
                    jobConsumer.accept(future.get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                } finally {
                    cdl.countDown();
                }
            });
        }
    }
}
