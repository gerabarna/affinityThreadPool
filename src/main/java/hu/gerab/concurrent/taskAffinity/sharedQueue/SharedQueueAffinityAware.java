package hu.gerab.concurrent.taskAffinity.sharedQueue;

import hu.gerab.concurrent.taskAffinity.AffinityAware;

public interface SharedQueueAffinityAware extends AffinityAware<String> {

    /**
     * Should return true only if there are no more instances belonging to this group
     * Any instance arriving after an instance with last flag set to true, is considered to be in a
     * new group which accidentally has the same id.
     *
     * @return
     */
    public boolean isLast();
}
