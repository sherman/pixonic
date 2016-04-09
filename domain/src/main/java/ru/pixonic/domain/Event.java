package ru.pixonic.domain;

import org.joda.time.DateTime;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * @author Denis Gabaydulin
 * @since 08/04/2016
 */
public class Event<R> {
    private final DateTime created;
    private final Callable<R> callable;

    public Event(DateTime created, Callable<R> callable) {
        this.created = created;
        this.callable = callable;
    }

    public DateTime getCreated() {
        return created;
    }

    public Callable<R> getCallable() {
        return callable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event<?> event = (Event<?>) o;
        return Objects.equals(created, event.created);
    }

    @Override
    public int hashCode() {
        return Objects.hash(created);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Event{");
        sb.append("created=").append(created);
        sb.append('}');
        return sb.toString();
    }
}
