package hu.gerab.concurrent.taskAffinity.hash;

import static hu.gerab.concurrent.taskAffinity.hash.HashingAffinityTask.create;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


/**
 * A Thread Pool Executor implementation that can enforce task execution order and thread use.
 * <p>
 * The ExecutorService guarantees that tasks implementing the {@link HashingAffinityAware} interface and returning the
 * same affinityId will be executed in submission order and on the same thread. {@link Runnable} instances not
 * implementing the {@link HashingAffinityAware} interface have no such guarantee regarding their execution order, they
 * may be executed on any thread in any order as in any executor service.
 */
public class HashingAffinityThreadPoolExecutor<K> extends ThreadPoolExecutor {

    public HashingAffinityThreadPoolExecutor(int poolSize, Supplier<BlockingQueue<Runnable>> queueFactory,
            ThreadFactory threadFactory) {
        super(poolSize, poolSize, 10, TimeUnit.MINUTES, new HashingAffinityQueue<>(poolSize, queueFactory), threadFactory);
        prestartAllCoreThreads();
    }

    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new HashingAffinityFutureTask<>(runnable, value);
    }

    protected <T> RunnableFuture<T> newTaskFor(Callable<T> runnable) {
        return new HashingAffinityFutureTask<>(runnable);
    }

    public Future<?> submit(K affinityId, Runnable task) {
        return super.submit(create(affinityId, task));
    }

    public <T> Future<T> submit(K affinityId, Callable<T> task) {
        return super.submit(create(affinityId, task));
    }
}
