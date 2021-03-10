package hu.gerab.concurrent.taskAffinity.hash;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

class HashingAffinityFutureTask<V> extends FutureTask<V> implements HashingAffinityAware {

    private HashingAffinityAware affinityAware;
    private final Object task;

    public HashingAffinityFutureTask(Callable<V> callable) {
        super(callable);
        task = callable;
        if (callable instanceof HashingAffinityAware) {
            affinityAware = (HashingAffinityAware) callable;
        }
    }

    public HashingAffinityFutureTask(Runnable runnable, V result) {
        super(runnable, result);
        task = runnable;
        if (runnable instanceof HashingAffinityAware) {
            affinityAware = (HashingAffinityAware) runnable;
        }
    }

    @Override
    public Object getAffinityId() {
        return affinityAware == null ? null : affinityAware.getAffinityId();
    }

    @Override
    public long getAffinityKey() {
        return affinityAware == null ? task.hashCode() : affinityAware.getAffinityKey();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return super.cancel(mayInterruptIfRunning);
    }

    @Override
    public String toString() {
        return "AffinityFutureTask{" +
                "affinityAware=" + affinityAware +
                "} ";
    }
}
