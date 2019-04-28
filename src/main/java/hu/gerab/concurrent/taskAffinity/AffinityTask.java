package hu.gerab.concurrent.taskAffinity;

import java.util.concurrent.Callable;

public interface AffinityTask extends AffinityAware {

    abstract class AbstractAffinityTask<T> implements AffinityTask {

        protected String affinityId;
        protected boolean last;
        protected T task;

        public AbstractAffinityTask(String affinityId, boolean last, T task) {
            this.affinityId = affinityId;
            this.last = last;
            this.task = task;
        }

        @Override
        public String getAffinityId() {
            return affinityId;
        }

        @Override
        public boolean isLast() {
            return last;
        }

        @Override
        public String toString() {
            return "AffinityTaskImpl{" +
                    "affinityId='" + affinityId + '\'' +
                    ", last=" + last +
                    ", target=" + task +
                    '}';
        }
    }

    public class RunnableAffinityTask extends AbstractAffinityTask<Runnable> implements Runnable {

        public RunnableAffinityTask(String affinityId, boolean last, Runnable target) {
            super(affinityId, last, target);
        }

        @Override
        public void run() {
            task.run();
        }
    }

    public class CallableAffinityTask<T> extends AbstractAffinityTask<Callable<T>> implements Callable<T> {

        public CallableAffinityTask(String affinityId, boolean last, Callable<T> target) {
            super(affinityId, last, target);
        }

        @Override
        public T call() throws Exception {
            return task.call();
        }
    }

    public static RunnableAffinityTask create(String id, boolean last, Runnable runnable) {
        return new RunnableAffinityTask(id, last, runnable);
    }

    public static <T> CallableAffinityTask<T> create(String id, boolean last, Callable<T> callable) {
        return new CallableAffinityTask<>(id, last, callable);
    }
}
