package hu.gerab.concurrent.taskAffinity.hash;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * This queue is designed to be used together with {@link HashingAffinityThreadPoolExecutor} and it's main goal is to
 * enforce affinity between {@link HashingAffinityAware} runnables. The queue internally maintains a separate
 * task queue for each thread and a shared queue for unknown affinity groups ( newly incoming ). Each thread will first
 * consume its own queue and only look at the shared queue if it's own queue is empty. This potentially could cause
 * tasks belonging 'new' ( unassigned ) affinity groups to be delayed
 */
class HashingAffinityQueue<R> implements BlockingQueue<R> {

    private final int threadCount;
    private final Supplier<BlockingQueue<R>> queueFactory;
    
    private final AtomicLong idGenerator = new AtomicLong();
    private final ConcurrentHashMap<Long, BlockingQueue<R>> poolIdToQueueMap = new ConcurrentHashMap<>();
    private final ThreadLocal<BlockingQueue<R>> threadQueue = ThreadLocal.withInitial(
            () -> getQueue(idGenerator.getAndIncrement()));

    public HashingAffinityQueue(int threadCount, Supplier<BlockingQueue<R>> queueFactory) {
        // we don't wanna create a lock for each affinity group as that could be a huge amount ->
        // instead we have a reasonable amount of locks that will be used when accessing affinity groups
        this.threadCount = threadCount;
        this.queueFactory = queueFactory;
    }

    private BlockingQueue<R> getQueueForThread() {
        return threadQueue.get();
    }

    private BlockingQueue<R> getQueue(Long poolId) {
        return poolIdToQueueMap.computeIfAbsent(poolId, id -> queueFactory.get());
    }

    private BlockingQueue<R> getQueue(R r) {
        final Long poolId = getPoolId(getAffinityKey(r));
        return getQueue(poolId);
    }

    private Long getAffinityKey(R a) {
        return ((HashingAffinityAware) a).getAffinityKey();
    }

    private Long getPoolId(Long affinityKey) {
        return affinityKey % threadCount;
    }

    @Override
    public void put(R r) throws InterruptedException {
        getQueue(r).put(r);
    }

    @Override
    public boolean offer(R r, long timeout, TimeUnit unit) throws InterruptedException {
        return getQueue(r).offer(r, timeout, unit);
    }

    @Override
    public boolean add(R r) {
        return getQueue(r).add(r);
    }

    @Override
    public boolean offer(R r) {
        return getQueue(r).add(r);
    }

    //////////////////////////////////// task retrieval ////////////////////////////////////////////

    @Override
    public R take() throws InterruptedException {
        return getQueueForThread().take();
    }

    @Override
    public R poll(long timeout, TimeUnit unit) throws InterruptedException {
        return getQueueForThread().poll(timeout, unit);
    }

    @Override
    public R poll() {
        return getQueueForThread().poll();
    }

    //////////////////////////////////////// unused ////////////////////////////////////////////////

    @Override
    public int remainingCapacity() {
        return getQueueForThread().remainingCapacity();
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
        return  poolIdToQueueMap.values().stream().mapToInt(Queue::size).sum();
    }

    @Override
    public boolean isEmpty() {
        return poolIdToQueueMap.values().stream().allMatch(Queue::isEmpty);
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
        c.forEach(this::add);
        return true;
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
                ", affinityGroupCount=" + poolIdToQueueMap.size() +
                '}';
    }
}
