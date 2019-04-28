package hu.gerab.concurrent.taskAffinity;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.hamcrest.CustomMatcher;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class AffinityThreadPoolExecutorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AffinityThreadPoolExecutorTest.class);
    private ThreadFactory threadFactory;
    private Queue<Future<TestData>> futures;
    private Queue<TestData> dataList;

    private static class TestData {

        private String threadName;
        private String affinityGroup;
        private int serial;

        public TestData(String affinityGroup, int serial) {
            this.threadName = Thread.currentThread().getName();
            this.affinityGroup = affinityGroup;
            this.serial = serial;
        }
    }

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
        ExecutorService executorService = new AffinityThreadPoolExecutor(1, threadFactory);

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
        ExecutorService executorService = new AffinityThreadPoolExecutor(1, threadFactory);

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
        ExecutorService executorService = new AffinityThreadPoolExecutor(10, threadFactory);

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
        ExecutorService executorService = new AffinityThreadPoolExecutor(10, threadFactory);

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
        ExecutorService executorService = new AffinityThreadPoolExecutor(10, threadFactory);

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

    private class OddMathcer extends CustomMatcher<Number> {

        public OddMathcer() {
            this("Expected an even number");
        }

        public OddMathcer(String description) {
            super(description);
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof Number) {
                if (o instanceof Short || o instanceof Integer || o instanceof Long) {
                    return ((long) ((Number) o) % 2) == 1;
                }
            }
            return false;
        }
    }

    @Test
    public void testCancelQueued() throws Exception {
        ExecutorService executorService = new AffinityThreadPoolExecutor(3, threadFactory);

        for (int i = 0; i < 3; i++) {
            executorService.submit(() -> {
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
        assertThat(dataList.stream().filter(td -> td.serial % 2 == 0).count(), equalTo(0L));
        assertThat(dataList.stream().filter(td -> td.serial % 2 == 1).count(), equalTo(15L));
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

    private void joinWithCancel() throws InterruptedException, java.util.concurrent.ExecutionException, TimeoutException {
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
            TestData oldData = affinityGroupToDataMap.put(testData.affinityGroup, testData);
            if (oldData != null) {
                assertThat("Incorrect order detected for group=" + testData.affinityGroup, oldData.serial, lessThan(testData.serial));
                assertThat("Thread change detected for group=" + testData.affinityGroup, oldData.threadName, equalTo(testData.threadName));
            }
        }
    }

    private abstract class TestAffinityTask implements AffinityTask, Callable<TestData> {

        protected String affinityGroup;
        protected int count;
        protected boolean last;

        public TestAffinityTask(String affinityGroup, int count, boolean last) {
            this.affinityGroup = affinityGroup;
            this.count = count;
            this.last = last;
        }

        @Override
        public String getAffinityId() {
            return affinityGroup;
        }

        @Override
        public boolean isLast() {
            return last;
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

    private Future<?> submitAffinityTask(ExecutorService executorService, String affinityGroup, int count, boolean last) {
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
        //        int sleepMillis = 50;
        sleep(sleepMillis);
        LOGGER.info("Executing for message group={}, serial={}", group, count);
        sleep(sleepMillis);
        return new TestData(group, count);
    }
}