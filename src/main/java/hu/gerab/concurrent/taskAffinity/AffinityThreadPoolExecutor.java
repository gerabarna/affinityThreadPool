package hu.gerab.concurrent.taskAffinity;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * A Thread Pool Executor implementation that can enforce task execution order and thread use.
 * <p>
 * The ExecutorService guarantees that tasks implementing the {@link AffinityAware} interface and returning
 * the same affinityId will be executed in submission order and on the same thread.
 * {@link Runnable} instances not implementing the {@link AffinityAware} interface have no such
 * guarantee regarding their execution order, they may be executed on any thread in any order as in
 * any executor service.
 */
public class AffinityThreadPoolExecutor extends ThreadPoolExecutor {

    public AffinityThreadPoolExecutor(int poolSize, ThreadFactory threadFactory) {
        super(poolSize, poolSize, 10, TimeUnit.MINUTES, new AffinityQueue<>(poolSize), threadFactory);
        prestartAllCoreThreads();
    }

    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return runnable instanceof AffinityAware
                ? new AffinityFutureTask<>(runnable, value)
                : new FutureTask<>(runnable, value);
    }

    protected <T> RunnableFuture<T> newTaskFor(Callable<T> runnable) {
        return runnable instanceof AffinityAware
                ? new AffinityFutureTask<>(runnable)
                : new FutureTask<>(runnable);
    }

    public Future<?> submit(String affinityId, boolean last, Runnable task) {
        return super.submit(AffinityTask.create(affinityId, last, task));
    }

    public <T> Future<T> submit(String affinityId, boolean last, Callable<T> task) {
        return super.submit(AffinityTask.create(affinityId, last, task));
    }
}
