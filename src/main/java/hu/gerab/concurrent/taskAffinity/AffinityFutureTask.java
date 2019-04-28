package hu.gerab.concurrent.taskAffinity;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

class AffinityFutureTask<V> extends FutureTask<V> implements AffinityAware {

    private AffinityAware affinityAware;

    public AffinityFutureTask(Callable<V> callable) {
        super(callable);
        if (callable instanceof AffinityAware) {
            affinityAware = (AffinityAware) callable;
        }
    }

    public AffinityFutureTask(Runnable runnable, V result) {
        super(runnable, result);
        if (runnable instanceof AffinityAware) {
            affinityAware = (AffinityAware) runnable;
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
