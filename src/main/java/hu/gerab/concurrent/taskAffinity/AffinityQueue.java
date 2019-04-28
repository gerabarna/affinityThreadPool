package hu.gerab.concurrent.taskAffinity;

import com.google.common.util.concurrent.Striped;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;

/**
 * This queue is designed to be used together with {@link AffinityThreadPoolExecutor} and it's main
 * goal is to enforce affinity between {@link AffinityAware} runnables.
 * The queue internally maintains a separate task queue for each thread and a shared queue for unknown
 * affinity groups ( newly incoming ). Each thread will first consume its own queue and only look at
 * the shared queue if it's own queue is empty. This potentially could cause tasks belonging 'new'
 * ( unassigned ) affinity groups to be delayed
 *
 * @param <R>
 */
class AffinityQueue<R extends Runnable> implements BlockingQueue<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AffinityQueue.class);

    private final LinkedBlockingQueue<R> sharedQueue = new LinkedBlockingQueue<>();

    private final Striped<Lock> insertLocks;

    private final ConcurrentHashMap<String, AffinityContext<R>> affinityIdToContextMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, AffinityContext<R>> threadIdToContextMap = new ConcurrentHashMap<>();

    public AffinityQueue(int threadCount) {
        // we don't wanna create a lock for each affinity group as that could be a huge amount ->
        // instead we have a reasonable amount of locks that will be used when accessing affinity groups
        insertLocks = Striped.lock(threadCount * 4);
    }


    @Override
    public void put(R r) throws InterruptedException {
        String affinityId = getAffinityId(r);

        if (affinityId == null) {
            put(r, noAffinityQueue(), affinityId);
            return;
        }
        Lock insertLock = insertLocks.get(affinityId);
        try {
            insertLock.lock();
            AffinityContext context = affinityIdToContextMap.get(affinityId);
            BlockingQueue<R> q = context == null ? sharedQueue : context.getQueue();
            LOGGER.trace("Put for context={}, result={}", context, r);
            put(r, q, affinityId);
        } finally {
            insertLock.unlock();
        }
    }

    @Override
    public boolean offer(R r, long timeout, TimeUnit unit) throws InterruptedException {
        String affinityId = getAffinityId(r);

        if (affinityId == null) {
            return offer(r, noAffinityQueue(), timeout, unit, affinityId);
        }
        Lock insertLock = insertLocks.get(affinityId);
        try {
            insertLock.lock();
            AffinityContext context = affinityIdToContextMap.get(affinityId);
            BlockingQueue<R> q = context == null ? sharedQueue : context.getQueue();
            LOGGER.trace("Put for context={}, result={}", context, r);
            return offer(r, q, timeout, unit, affinityId);
        } finally {
            insertLock.unlock();
        }
    }

    private <T> T insertOperation(R r, BiFunction<Queue<R>, R, T> operation) {
        String affinityId = getAffinityId(r);
        if (affinityId == null) {
            return insert(r, noAffinityQueue(), operation, affinityId);
        }
        Lock insertLock = insertLocks.get(affinityId);
        try {
            insertLock.lock();
            AffinityContext context = affinityIdToContextMap.get(affinityId);
            BlockingQueue<R> q = context == null ? sharedQueue : context.getQueue();
            LOGGER.trace("Insert for context={}, result={}", context, r);
            return insert(r, q, operation, affinityId);
        } finally {
            insertLock.unlock();
        }
    }

    private <T> T insert(R r, BlockingQueue<R> q, BiFunction<Queue<R>, R, T> operation, String affinityId) {
        T t = operation.apply(q, r);
        afterInsert(q, affinityId);
        return t;
    }

    @Override
    public boolean add(R a) {
        return insertOperation(a, Queue::add);
    }

    @Override
    public boolean offer(R a) {
        return insertOperation(a, Queue::offer);
    }

    private void put(R r, BlockingQueue<R> q, String affinityId) throws InterruptedException {
        q.put(r);
        afterInsert(q, affinityId);
    }

    private boolean offer(R r, BlockingQueue<R> q, long timeout, TimeUnit unit, String affinityId) throws InterruptedException {
        boolean offer = q.offer(r, timeout, unit);
        afterInsert(q, affinityId);
        return offer;
    }

    private String getAffinityId(R a) {
        if (a instanceof AffinityAware) {
            return ((AffinityAware) a).getAffinityId();
        }
        return null;
    }

    private void afterInsert(BlockingQueue<R> insertedQueue, String affinityId) {
        if (affinityId == null || insertedQueue == sharedQueue) {
            // This might return a thread that is currently still running it's last task, but that should not
            // be a problem as tasks will try to get a new task again after they have finished their previous one
            getIdleContext().ifPresent(AffinityContext::interrupt);
        }
    }

    private BlockingQueue<R> noAffinityQueue() {
        return getIdleContext().map(AffinityContext::getQueue).orElse(sharedQueue);
    }

    private Optional<AffinityContext<R>> getIdleContext() {
        return threadIdToContextMap.values().stream().unordered()
                .filter(c -> c.isWaiting() && c.getQueue().isEmpty())
                .findAny();
    }

    private AffinityContext<R> getContextForThread() {
        long id = Thread.currentThread().getId();
        return threadIdToContextMap.computeIfAbsent(id, AffinityContext::new);
    }

    private BlockingQueue<R> getQueueForThread() {
        return getContextForThread().getQueue();
    }

    //////////////////////////////////// task retrieval ////////////////////////////////////////////

    private void fillQueueIfEmpty(AffinityContext<R> context) {
        BlockingQueue<R> queue = context.getQueue();
        if (queue.isEmpty() && !sharedQueue.isEmpty()) {
            synchronized (sharedQueue) {
                R r = sharedQueue.poll();
                if (r != null) {
                    if (r instanceof AffinityAware) {
                        acquireNewAffintyGroup(context, r);
                    } else {
                        queue.offer(r);
                    }
                }
            }
        }
        if (queue.isEmpty()) {
            context.setWaiting(true);
        }
    }

    private void acquireNewAffintyGroup(AffinityContext<R> context, R r) {
        BlockingQueue<R> queue = context.getQueue();
        String affinityId = ((AffinityAware) r).getAffinityId();
        Lock insertLock = insertLocks.get(affinityId);
        insertLock.lock();
        try {
            AffinityContext previous = affinityIdToContextMap.put(affinityId, context);
            if (previous != null) {
                LOGGER.warn("Affinity context override from={}, to={}", previous.getName(), context.getName());
            }
            queue.offer(r);
            moveAllFromSharedToQueue(queue, affinityId);
        } finally {
            insertLock.unlock();
        }
    }

    private void moveAllFromSharedToQueue(BlockingQueue<R> queue, String affinityId) {
        Iterator<R> iterator = sharedQueue.iterator();
        while (iterator.hasNext()) {
            R task = iterator.next();
            if (task instanceof AffinityAware
                    && ((AffinityAware) task).getAffinityId().equals(affinityId)) {
                iterator.remove();
                queue.offer(task);
            }
        }
    }

    @FunctionalInterface
    public interface InterruptibleFunction<T, R> {

        R apply(T t) throws InterruptedException;
    }

    private R get(InterruptibleFunction<BlockingQueue<R>, R> operation) {
        AffinityContext<R> context = getContextForThread();
        int loopcounter = 0;
        while (true) { // we exit the loop with the return statement
            fillQueueIfEmpty(context);
            try {
                R r = operation.apply(context.getQueue());
                context.setWaiting(false);
                LOGGER.trace("Get for thread={}, result={}", context.getName(), r);
                if (r instanceof AffinityAware && ((AffinityAware) r).isLast()) {
                    String affinityId = ((AffinityAware) r).getAffinityId();
                    Lock lock = insertLocks.get(affinityId);
                    try {
                        lock.lock();
                        affinityIdToContextMap.remove(affinityId);
                    } finally {
                        lock.unlock();
                    }
                }
                return r;
            } catch (InterruptedException e) {
                context.setWaiting(false);
                LOGGER.trace("Interruption loop: " + ++loopcounter);
            }
        }
    }

    @Override
    public R take() throws InterruptedException {
        return get(BlockingQueue::take);
    }

    @Override
    public R poll(long timeout, TimeUnit unit) throws InterruptedException {
        return get(q -> q.poll(timeout, unit));
    }

    @Override
    public R poll() {
        return get(Queue::poll);
    }

    //////////////////////////////////////// unused ////////////////////////////////////////////////

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public R remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public R element() {
        throw new UnsupportedOperationException();
    }

    @Override
    public R peek() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return getQueueForThread().size();
    }

    @Override
    public boolean isEmpty() {
        return getQueueForThread().isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super R> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super R> c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<R> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends R> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "AffinityQueue{" +
                "sharedQueueSize=" + sharedQueue.size() +
                ", contextCount=" + affinityIdToContextMap.size() +
                ", affinityGroupCount=" + threadIdToContextMap.size() +
                '}';
    }
}
