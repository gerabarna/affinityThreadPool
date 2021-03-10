package hu.gerab.concurrent.taskAffinity.hash;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import hu.gerab.concurrent.taskAffinity.TestData;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashingAffinityThreadPoolExecutorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashingAffinityThreadPoolExecutorTest.class);
    private ThreadFactory threadFactory;
    private Queue<Future<TestData>> futures;
    private Queue<TestData> dataList;

    @Before
    public void setUp() throws Exception {
        threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("QueueTest-%d")
                .build();
        futures = new ConcurrentLinkedQueue<>();
        dataList = new ConcurrentLinkedQueue<>();
    }

    @Test
    public void testNormalRunnableDoesntThrow() throws Exception {
        ExecutorService executorService = new HashingAffinityThreadPoolExecutor<>(1, LinkedBlockingQueue::new,
                threadFactory);

        for (int i = 0; i <= 10; i++) {
            executorService.submit(createRunnableTask("Mike", i));
            executorService.submit(createRunnableTask("Leo", i));
            executorService.submit(createRunnableTask("Don", i));
            executorService.submit(createRunnableTask("Raph", i));
        }
        join();
    }

    @Test
    public void testSingleProducerSingleConsumer() throws Exception {
        ExecutorService executorService = new HashingAffinityThreadPoolExecutor<>(1, LinkedBlockingQueue::new,
                threadFactory);

        int limit = 10;
        for (int i = 0; i <= limit; i++) {
            executorService.submit(createAffinityTask("Mike", i, i == limit));
            executorService.submit(createAffinityTask("Leo", i, i == limit));
            executorService.submit(createAffinityTask("Don", i, i == limit));
            executorService.submit(createAffinityTask("Raph", i, i == limit));
        }

        join();
        assertResultOrder();
    }

    @Test
    public void testSingleProducerMultiConsumer() throws Exception {
        ExecutorService executorService = new HashingAffinityThreadPoolExecutor<>(10, LinkedBlockingQueue::new,
                threadFactory);

        int limit = 10;
        for (int i = 0; i <= limit; i++) {
            executorService.submit(createAffinityTask("Mike", i, i == limit));
            executorService.submit(createAffinityTask("Leo", i, i == limit));
        }

        for (int i = 0; i <= limit; i++) {
            executorService.submit(createAffinityTask("Don", i, i == limit));
            executorService.submit(createAffinityTask("Raph", i, i == limit));
        }

        join();
        assertResultOrder();
    }

    @Test
    public void testSingleProducerMultiConsumerWithNormalRunnables() throws Exception {
        ExecutorService executorService = new HashingAffinityThreadPoolExecutor<>(10, LinkedBlockingQueue::new,
                threadFactory);

        int limit = 10;
        for (int i = 0; i <= limit; i++) {
            executorService.submit(createAffinityTask("Mike", i, i == limit));
            executorService.submit(createAffinityTask("Leo", i, i == limit));
        }

        for (int i = 0; i <= limit; i++) {
            executorService.submit(createRunnableTask("April", i));
            executorService.submit(createAffinityTask("Don", i, i == limit));
            executorService.submit(createAffinityTask("Raph", i, i == limit));
        }

        for (int i = 0; i <= limit; i++) {
            executorService.submit(createRunnableTask("Casey", i));
        }

        join();
        assertResultOrder();
    }

    @Test
    public void testMultiProducerMultiConsumer() throws Exception {
        ExecutorService executorService = new HashingAffinityThreadPoolExecutor<>(10, LinkedBlockingQueue::new,
                threadFactory);

        executorService.submit(createProducerTask(executorService, 10, "Mike"));
        executorService.submit(createProducerTask(executorService, 10, "Leo"));
        executorService.submit(createProducerTask(executorService, 10, "Don"));
        executorService.submit(createProducerTask(executorService, 10, "Raph"));

        for (int i = 0; i < 10; i++) {
            executorService.submit(createRunnableTask("Casey", i));
        }

        join();
        assertResultOrder();
    }

    @Test
    public void testCancelQueued() throws Exception {
        HashingAffinityThreadPoolExecutor executorService = new HashingAffinityThreadPoolExecutor<>(3,
                LinkedBlockingQueue::new, threadFactory);

//        Stream.of("Mike", "Leo", "Don").forEach(i->
        for (int i = 0; i < 3; i++) {
            executorService.submit(i, () -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted initial sleep cycle");
                }
            });
        }

        for (int i = 0; i <= 10; i++) {
            Future<?> mike = submitAffinityTask(executorService, "Mike", i, false);
            Future<?> leo = submitAffinityTask(executorService, "Leo", i, false);
            Future<?> don = submitAffinityTask(executorService, "Don", i, false);
            if (i % 2 == 0) {
                mike.cancel(true);
                leo.cancel(true);
                don.cancel(true);
            }
        }
        joinWithCancel();
        assertResultOrder();
        assertThat(dataList.stream().filter(td -> td.getSerial() % 2 == 0).count(), equalTo(0L));
        assertThat(dataList.stream().filter(td -> td.getSerial() % 2 == 1).count(), equalTo(15L));
    }

    private Runnable createProducerTask(ExecutorService executorService, int taskCount, String affinityGroup) {
        return () -> {
            for (int i = 0; i <= taskCount; i++) {
                executorService.submit(createAffinityTask(affinityGroup, i, i == taskCount));
                LOGGER.info("Submitted task for={}, count={}", affinityGroup, i);
            }
        };
    }

    private void join() throws InterruptedException, java.util.concurrent.ExecutionException, TimeoutException {
        for (Future future : futures) {
            future.get(10, SECONDS);
        }
    }

    private void joinWithCancel()
            throws InterruptedException, java.util.concurrent.ExecutionException, TimeoutException {
        for (Future future : futures) {
            try {
                future.get(10, SECONDS);
            } catch (CancellationException e) {
                LOGGER.info("Task cancelled:{}", e.getMessage());
            }
        }
    }

    private void assertResultOrder() {
        Map<String, TestData> affinityGroupToDataMap = new HashMap<>();
        for (TestData testData : dataList) {
            TestData oldData = affinityGroupToDataMap.put(testData.getAffinityGroup(), testData);
            if (oldData != null) {
                assertThat("Incorrect order detected for group=" + testData.getAffinityGroup(), oldData.getSerial(),
                        lessThan(testData.getSerial()));
                assertThat("Thread change detected for group=" + testData.getAffinityGroup(), oldData.getThreadName(),
                        equalTo(testData.getThreadName()));
            }
        }
    }

    private abstract class TestAffinityTask implements HashingAffinityTask, Callable<TestData> {

        protected String affinityGroup;
        protected long affinityKey;
        protected int count;
        protected boolean last;

        public TestAffinityTask(String affinityGroup, int count, boolean last) {
            this.affinityGroup = affinityGroup;
            affinityKey = affinityGroup.hashCode();
            this.count = count;
            this.last = last;
        }

        @Override
        public Object getAffinityId() {
            return affinityGroup;
        }

        @Override
        public long getAffinityKey() {
            return affinityKey;
        }

        @Override
        public String toString() {
            return "AffinityTask{"
                    + "affinityGroup=" + affinityGroup
                    + ", count=" + count
                    + "}";
        }
    }

    private TestAffinityTask createAffinityTask(String affinityGroup, int count, boolean last) {
        CompletableFuture future = new CompletableFuture();
        futures.add(future);
        return new TestAffinityTask(affinityGroup, count, last) {
            @Override
            public TestData call() {
                TestData data = task(affinityGroup, count, future);
                dataList.add(data);
                return data;
            }
        };
    }

    private Future<?> submitAffinityTask(ExecutorService executorService, String affinityGroup, int count,
            boolean last) {
        TestAffinityTask testAffinityTask = new TestAffinityTask(affinityGroup, count, last) {
            @Override
            public TestData call() {
                TestData data = task(affinityGroup, count);
                dataList.add(data);
                return data;
            }
        };
        Future<TestData> future = executorService.submit(testAffinityTask);
        futures.add(future);
        return future;
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOGGER.warn("Task sleep cycle interrupted!");
        }
    }

    private Runnable createRunnableTask(String affinityGroup, int count) {
        CompletableFuture<TestData> future = new CompletableFuture<>();
        futures.add(future);
        return () -> task(affinityGroup, count, future);
    }

    private TestData task(String group, int count, CompletableFuture<TestData> future) {
        TestData testData = task(group, count);
        future.complete(testData);
        return testData;
    }

    private TestData task(String group, int count) {
        int sleepMillis = (int) (Math.random() * 8 + 1) * 10;
        sleep(sleepMillis);
        LOGGER.info("Executing for message group={}, serial={}", group, count);
        sleep(sleepMillis);
        return new TestData(group, count);
    }
}