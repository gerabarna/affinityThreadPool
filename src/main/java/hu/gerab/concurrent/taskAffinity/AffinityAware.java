package hu.gerab.concurrent.taskAffinity;

import hu.gerab.concurrent.taskAffinity.sharedQueue.SharedQueueAffinityThreadPoolExecutor;

/**
 * This interface is intended to be used to indicate a grouping of sorts.
 * It is particularly useful in conjunction with the {@link SharedQueueAffinityThreadPoolExecutor} where tasks
 * implementing this interface and belonging to the same affinity group are guaranteed to be
 * executed on the same thread in submission order.
 */
public interface AffinityAware<K> {

    /**
     * Should return a value identifying the group this instance belongs to.
     *
     * @return
     */
    public K getAffinityId();
}
