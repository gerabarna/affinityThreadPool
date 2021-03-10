package hu.gerab.concurrent.taskAffinity.hash;

import java.util.concurrent.Callable;

public interface HashingAffinityTask extends HashingAffinityAware {

    public abstract class AbstractAffinityTask<T> implements HashingAffinityTask {

        protected Object affinityId;

        protected T task;

        private AbstractAffinityTask(Object affinityKey, T task) {
            this.affinityId = affinityKey;
            this.task = task;
        }

        @Override
        public Object getAffinityId() {
            return affinityId;
        }

        @Override
        public long getAffinityKey() {
            if (affinityId == null) {
                return task.hashCode();
            } else if (affinityId instanceof Long) {
                return (long) affinityId;
            } else if (affinityId instanceof Integer) {
                return (Integer) affinityId;
            } else {
                return affinityId.hashCode();
            }
        }

        @Override
        public String toString() {
            return "AbstractAffinityTask{" +
                    "affinityId=" + affinityId +
                    ", affinityKey=" + affinityId +
                    ", task=" + task +
                    '}';
        }
    }

    public class RunnableAffinityTask extends AbstractAffinityTask<Runnable> implements Runnable {

        public RunnableAffinityTask(Object affinityId, Runnable target) {
            super(affinityId, target);
        }

        @Override
        public void run() {
            task.run();
        }
    }

    public class CallableAffinityTask<T> extends AbstractAffinityTask<Callable<T>> implements Callable<T> {

        public CallableAffinityTask(long affinityId, Callable<T> target) {
            super(affinityId, target);
        }

        @Override
        public T call() throws Exception {
            return task.call();
        }
    }

    public static RunnableAffinityTask create(Long id, Runnable runnable) {
        return new RunnableAffinityTask(id, runnable);
    }

    public static <T> CallableAffinityTask<T> create(Long id, Callable<T> callable) {
        return new CallableAffinityTask<>(id, callable);
    }

    public static RunnableAffinityTask create(long id, Runnable runnable) {
        return create(Long.valueOf(id), runnable);
    }

    public static <T> CallableAffinityTask<T> create(long id, Callable<T> callable) {
        return create(Long.valueOf(id), callable);
    }

    public static RunnableAffinityTask create(int id, Runnable runnable) {
        return create(Long.valueOf(id), runnable);
    }

    public static <T> CallableAffinityTask<T> create(int id, Callable<T> callable) {
        return create(Long.valueOf(id), callable);
    }

    public static RunnableAffinityTask create(Integer id, Runnable runnable) {
        return create(Long.valueOf(id), runnable);
    }

    public static <T> CallableAffinityTask<T> create(Integer id, Callable<T> callable) {
        return create(Long.valueOf(id), callable);
    }

    public static RunnableAffinityTask create(Object id, Runnable runnable) {
        return create(Long.valueOf(id.hashCode()), runnable);
    }

    public static <T> CallableAffinityTask<T> create(Object id, Callable<T> callable) {
        return create(Long.valueOf(id.hashCode()), callable);
    }
}
