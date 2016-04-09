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

    private final ScheduledThreadPoolExecutor scheduledExecutor;
    private final ExecutorService lateTaskExecutor;
    private final BlockingQueue<Runnable> queue;
    private final int maxQueueSize;
    private final Histogram drifts;

    public ConsumerServiceImpl(int maxQueueSize, int maxThreads, int lateMaxThreads) {
        // FIXME: find optimal queue length for late task executor
        queue = new PriorityBlockingQueue<>(maxQueueSize / 2, new EventComparator());

        MetricRegistry registry = new MetricRegistry();
        drifts = registry.histogram("drifts");

        Slf4jReporter
                .forRegistry(registry)
                .outputTo(log)
                .build()
                .start(10, TimeUnit.SECONDS);

        this.maxQueueSize = maxQueueSize;
        scheduledExecutor = createExecutor(maxThreads);
        lateTaskExecutor = createLateTaskExecutor(lateMaxThreads);
    }

    @Override
    public void addEvent(@NotNull Event<?> event) throws LimitIsReachedException {
        if (scheduledExecutor.getQueue().size() + queue.size() >= maxQueueSize) {
            throw new LimitIsReachedException();
        }

        if (event.getCreated().isBeforeNow()) {
            // task in the past, run it immediately
            lateTaskExecutor.submit(new CallableWrapper<>(event));
        } else {
            scheduledExecutor.schedule(
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

    private ThreadPoolExecutor createLateTaskExecutor(int threads) {
        return new CustomThreadPoolExecutor(threads, threads,
                0L, TimeUnit.MILLISECONDS,
                queue,
                new ServiceThreadFactory());
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

        public Event<R> getDelegate() {
            return delegate;
        }
    }

    private static class CustomThreadPoolExecutor extends ThreadPoolExecutor {
        public CustomThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            if (!(callable instanceof CallableWrapper)) {
                throw new IllegalArgumentException("Strange callable given!");
            }

            return new PriorityFutureTask<T>((CallableWrapper)callable);
        }
    }

    private static class PriorityFutureTask<V> extends FutureTask<V> {
        private final CallableWrapper wrapper;

        public PriorityFutureTask(CallableWrapper<V> callable) {
            super(callable);
            wrapper = callable;
        }

        public PriorityFutureTask(Runnable runnable, V result) {
            super(runnable, result);
            throw new UnsupportedOperationException("");
        }

        public CallableWrapper getWrapper() {
            return wrapper;
        }
    }

    private class EventComparator implements Comparator<Runnable> {
        @Override
        public int compare(Runnable o1, Runnable o2) {
            PriorityFutureTask<?> task1 = ((PriorityFutureTask<?>) o1);
            PriorityFutureTask<?> task2 = ((PriorityFutureTask<?>) o2);

            return task1.getWrapper().getDelegate().getCreated().compareTo(task2.getWrapper().getDelegate().getCreated());
        }
    }
}
