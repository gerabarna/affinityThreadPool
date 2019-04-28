package hu.gerab.concurrent.taskAffinity;

/**
 * This interface is intended to be used to indicate a grouping of sorts.
 * It is particularly useful in conjunction with the {@link AffinityThreadPoolExecutor} where tasks
 * implementing this interface and belonging to the same affinity group are guaranteed to be
 * executed on the same thread in submission order.
 */
public interface AffinityAware {

    /**
     * Should return a value identifying the group this instance belongs to.
     *
     * @return
     */
    public String getAffinityId();

    /**
     * Should return true only if there are no more instances belonging to this group
     * Any instance arriving after an instance with last flag set to true, is considered to be in a
     * new group which accidentally has the same id.
     *
     * @return
     */
    public boolean isLast();
}
