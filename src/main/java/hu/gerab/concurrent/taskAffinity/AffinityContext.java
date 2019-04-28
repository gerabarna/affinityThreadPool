package hu.gerab.concurrent.taskAffinity;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class AffinityContext<R> implements Comparable<AffinityContext> {

    private String name;
    private BlockingQueue<R> threadQueue = new LinkedBlockingQueue<>();
    private boolean waiting = true;
    private final Thread thread;

    protected AffinityContext(long threadId) {
        this();
    }

    protected AffinityContext() {
        thread = Thread.currentThread();
        name = thread.getName();
    }

    @Override
    public int compareTo(AffinityContext o) {
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
