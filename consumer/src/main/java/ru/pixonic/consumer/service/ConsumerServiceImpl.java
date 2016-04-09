package ru.pixonic.consumer.service;

import com.codahale.metrics.*;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.pixonic.domain.Event;

import java.util.Comparator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTime.now;
import static org.joda.time.DateTimeZone.UTC;

/**
 * @author Denis Gabaydulin
 * @since 08/04/2016
 */
public class ConsumerServiceImpl implements ConsumerService {
    private static final Logger log = LoggerFactory.getLogger(ConsumerServiceImpl.class);


    private final ScheduledThreadPoolExecutor executorService;
    private final int maxQueueSize;
    private final Histogram drifts;

    public ConsumerServiceImpl(int maxQueueSize, int maxThreads) {
        MetricRegistry registry = new MetricRegistry();
        drifts = registry.histogram("drifts");

        Slf4jReporter
                .forRegistry(registry)
                .outputTo(log)
                .build()
                .start(10, TimeUnit.SECONDS);

        this.maxQueueSize = maxQueueSize;
        executorService = createExecutor(maxThreads);
    }

    @Override
    public void addEvent(@NotNull Event<?> event) throws LimitIsReachedException {
        if (executorService.getQueue().size() >= maxQueueSize) {
            throw new LimitIsReachedException();
        }

        if (event.getCreated().isBeforeNow()) {
            // task in the past, run it immediately
            executorService.submit(new CallableWrapper<>(event));
        } else {
            executorService.schedule(
                    new CallableWrapper<>(event),
                    event.getCreated().minus(now(UTC).getMillis()).getMillis(),
                    MILLISECONDS
            );
        }
    }

    private ScheduledThreadPoolExecutor createExecutor(int threads) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(threads, new ServiceThreadFactory());
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private static class ServiceThreadFactory implements ThreadFactory {
        private static final String nameFormat = "Consumer-%d";
        private static final AtomicLong count = new AtomicLong();

        @Override
        public Thread newThread(@NotNull Runnable r) {
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setName(String.format(nameFormat, count.getAndIncrement()));
            thread.setDaemon(true);
            return thread;
        }
    }

    private class CallableWrapper<R> implements Callable<R> {
        private final Event<R> delegate;

        public CallableWrapper(Event<R> delegate) {
            this.delegate = delegate;
        }

        @Override
        public R call() throws Exception {
            drifts.update(Math.abs(DateTime.now(UTC).getMillis() - delegate.getCreated().getMillis()));

            return delegate.getCallable().call();
        }
    }
}
