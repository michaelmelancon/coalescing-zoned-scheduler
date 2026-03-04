package com.example.scheduler;

import java.time.Instant;
import java.util.List;

public interface CoalescingZonedScheduler<K, P> {

    /**
     * Schedules a key for immediate processing.
     * <ul>
     * <li>Not scheduled → insert into immediate zone</li>
     * <li>Already immediate → no-op</li>
     * <li>Currently observed → promote to immediate</li>
     * </ul>
     */
    void scheduleNow(K key, P payload);

    /**
     * Schedules a key for delayed processing at the given instant.
     * <ul>
     * <li>Not scheduled → insert into delayed zone</li>
     * <li>Already immediate → no-op</li>
     * <li>Already delayed → update timestamp and payload in place semantically via
     * rewrite</li>
     * </ul>
     */
    void scheduleLater(K key, P payload, Instant when);

    /**
     * Drains up to {@code limit} currently eligible entries: all immediate entries
     * plus deferred entries scheduled strictly before
     * {@code delayedCutoffExclusive}.
     */
    List<ScheduledItem<K, P>> drain(int limit, Instant delayedCutoffExclusive);

    /**
     * Drains up to {@code limit} currently eligible entries: all immediate entries
     * plus deferred entries scheduled before {@code Instant.EPOCH}.
     * 
     * @param limit
     * @return
     */
    default List<ScheduledItem<K, P>> drain(int limit) {
        return drain(limit, Instant.now());
    }

    /**
     * Cancels the current schedule for the key, if present.
     *
     * @return true if an entry was removed; false otherwise
     */
    boolean cancel(K key);

    boolean isScheduled(K key);

    long sizeImmediate();

    long sizeDelayed();

    long sizeTotal();

}