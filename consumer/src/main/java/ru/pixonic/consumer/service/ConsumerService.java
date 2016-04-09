package ru.pixonic.consumer.service;

import org.jetbrains.annotations.NotNull;
import ru.pixonic.domain.Event;

/**
 * @author Denis Gabaydulin
 * @since 08/04/2016
 */
public interface ConsumerService {
    /**
     * @throws LimitIsReachedException if the scheduler queue limit is reached
     */
    void addEvent(@NotNull Event<?> event) throws LimitIsReachedException;
}
