package ru.pixonic.consumer;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import ru.pixonic.consumer.service.ConsumerService;
import ru.pixonic.consumer.service.ConsumerServiceImpl;
import ru.pixonic.domain.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.joda.time.DateTime.now;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Denis Gabaydulin
 * @since 08/04/2016
 */
public class ConsumerTest {
    private static final Logger log = LoggerFactory.getLogger(ConsumerTest.class);

    private static final ExecutorService helperExecutor = Executors.newFixedThreadPool(64);

    @Test
    public void simpleOrder() throws InterruptedException {
        ConsumerService service = new ConsumerServiceImpl(1024, 16);

        List<Integer> result = new ArrayList<>();
        Supplier<Integer> nextIdFunc = new MonotonicIncSupplier(1);
        Supplier<Integer> nextDelayFunc = new MonotonicIncSupplier(10);

        int max = 10;
        CountDownLatch latch = new CountDownLatch(max);

        for (int i = 0; i < max; i++) {
            service.addEvent(getEvent(latch, result, nextIdFunc, nextDelayFunc));
        }

        latch.await();

        assertEquals(result, asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    }

    @Test
    public void reverseOrder() throws InterruptedException {
        ConsumerService service = new ConsumerServiceImpl(1024, 16);

        List<Integer> result = new ArrayList<>();
        Supplier<Integer> nextIdFunc = new MonotonicIncSupplier(1);
        Supplier<Integer> nextDelayFunc = new MonotonicDecSupplier(2000, 100);

        int max = 10;
        CountDownLatch latch = new CountDownLatch(max);

        for (int i = 0; i < max; i++) {
            service.addEvent(getEvent(latch, result, nextIdFunc, nextDelayFunc));
        }

        latch.await();

        assertEquals(result, asList(10, 9, 8, 7, 6, 5, 4, 3, 2, 1));
    }

    /**
     * This test just for playing with drift
     */
    @Test(invocationCount = 1)
    public void delayed() throws InterruptedException {
        ConsumerService service = new ConsumerServiceImpl(60000, 1);

        // executor is overloaded
        /*for (int i = 0; i < 8; i++) {
            service.addEvent(stub);
        }*/

        List<Integer> result = new ArrayList<>();
        Supplier<Integer> nextIdFunc = new MonotonicIncSupplier(1);
        Supplier<Integer> nextDelayFunc = new MonotonicDecSupplier(15000, 10);

        CountDownLatch latch = new CountDownLatch(1000);

        for (int i = 0; i < 1000; i++) {
            helperExecutor.submit(() -> service.addEvent(getEvent(latch, result, nextIdFunc, nextDelayFunc)));
        }

        latch.await();

        log.info("{}", result);

        Thread.sleep(10000);
    }

    private static Event<Integer> stub = new Event<>(now(UTC), () -> {
        Thread.sleep(2000);
        return 42;
    });

    private Event<Integer> getEvent(
            CountDownLatch condition,
            List<Integer> result,
            Supplier<Integer> nextIdFunc,
            Supplier<Integer> nextDelayFunc
    ) {
        Integer id = nextIdFunc.get();
        DateTime time = now(UTC).plus(nextDelayFunc.get());
        return new Event<>(
                time,
                () -> {
                    try {
                        log.info("{} {}", id, time);
                        synchronized (result) {
                            result.add(id);
                        }
                        return 42;
                    } finally {
                        condition.countDown();
                    }
                });
    }

    private static class MonotonicIncSupplier implements Supplier<Integer> {
        private final AtomicInteger counter = new AtomicInteger();
        private final int stepMills;

        public MonotonicIncSupplier(int stepMills) {
            this.stepMills = stepMills;
        }

        @Override
        public Integer get() {
            return counter.addAndGet(stepMills);
        }
    }

    private static class MonotonicDecSupplier implements Supplier<Integer> {
        private final AtomicInteger counter;
        private final int stepMills;

        public MonotonicDecSupplier(int initial, int stepMills) {
            counter = new AtomicInteger(initial);
            this.stepMills = stepMills;
        }

        @Override
        public Integer get() {
            return counter.addAndGet(-1 * stepMills);
        }
    }
}