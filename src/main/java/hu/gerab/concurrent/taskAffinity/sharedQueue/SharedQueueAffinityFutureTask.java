package hu.gerab.concurrent.taskAffinity.sharedQueue;

import hu.gerab.concurrent.taskAffinity.AffinityAware;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

class SharedQueueAffinityFutureTask<V> extends FutureTask<V> implements SharedQueueAffinityAware {

    private SharedQueueAffinityAware affinityAware;

    public SharedQueueAffinityFutureTask(Callable<V> callable) {
        super(callable);
        if (callable instanceof SharedQueueAffinityAware) {
            affinityAware = (SharedQueueAffinityAware) callable;
        }
    }

    public SharedQueueAffinityFutureTask(Runnable runnable, V result) {
        super(runnable, result);
        if (runnable instanceof SharedQueueAffinityAware) {
            affinityAware = (SharedQueueAffinityAware) runnable;
        }
    }

    @Override
    public String getAffinityId() {
        return affinityAware == null ? null : affinityAware.getAffinityId();
    }

    @Override
    public boolean isLast() {
        return affinityAware != null && affinityAware.isLast();
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
