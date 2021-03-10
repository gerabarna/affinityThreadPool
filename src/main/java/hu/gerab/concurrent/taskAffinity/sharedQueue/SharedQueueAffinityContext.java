package hu.gerab.concurrent.taskAffinity.sharedQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class SharedQueueAffinityContext<R> implements Comparable<SharedQueueAffinityContext> {

    private String name;
    private BlockingQueue<R> threadQueue = new LinkedBlockingQueue<>();
    private volatile boolean waiting = true;
    private final Thread thread;

    protected SharedQueueAffinityContext() {
        thread = Thread.currentThread();
        name = thread.getName();
    }

    @Override
    public int compareTo(SharedQueueAffinityContext o) {
        return Integer.compare(threadQueue.size(), o.threadQueue.size());
    }

    @Override
    public String toString() {
        return "AffinityContext{" +
                "name='" + name + '\'' +
                ", queueSize=" + threadQueue.size() +
                ", waiting=" + waiting +
                '}';
    }

    public String getName() {
        return name;
    }

    public BlockingQueue<R> getQueue() {
        return threadQueue;
    }

    public boolean isEmpty() {
        return threadQueue.isEmpty();
    }

    public int size() {
        return threadQueue.size();
    }

    public boolean isWaiting() {
        return waiting;
    }

    public void setWaiting(boolean waiting) {
        this.waiting = waiting;
    }

    public void interrupt() {
        thread.interrupt();
    }
}
