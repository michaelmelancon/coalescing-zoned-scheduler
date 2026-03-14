/*
 * Copyright 2026 Michael Melancon
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.melancon.scheduler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * A persistent keyed scheduler backed by a {@link SchedulerStore}.
 *
 * <p>
 * The scheduler maintains at most one live scheduled entry per logical key.
 * Immediate entries drain before delayed entries. Re-scheduling coalesces by
 * key rather than preserving every enqueue.
 *
 * <h2>Zone layout</h2>
 * <table>
 * <caption>Zone layout within the backing store</caption>
 * <tr>
 * <th>Zone</th>
 * <th>Position range</th>
 * <th>Purpose</th>
 * </tr>
 * <tr>
 * <td>Immediate</td>
 * <td>[2, 1B)</td>
 * <td>FIFO-like ordering using a recyclable counter</td>
 * </tr>
 * <tr>
 * <td>Delayed</td>
 * <td>[1B, ∞)</td>
 * <td>Time-ordered scheduling using epoch seconds</td>
 * </tr>
 * </table>
 *
 * <p>
 * This payload-carrying form is appropriate only when coalescing by logical key
 * is correct. If the same key must preserve multiple outstanding work items
 * independently, this is the wrong abstraction.
 */
public class KeyValueStoreCoalescingZonedScheduler<K, P> implements CoalescingZonedScheduler<K, P> {

    static final long FIRST_IMMEDIATE_POSITION = 2L;
    static final long DELAYED_BOUNDARY = 1_000_000_000L;

    private static final String NEXT_IMMEDIATE_POSITION_KEY = "next-immediate-position";
    private static final String IMMEDIATE_COUNT_KEY = "immediate-count";
    private static final String DELAYED_COUNT_KEY = "delayed-count";

    private final SchedulerStore<K, P> store;

    public KeyValueStoreCoalescingZonedScheduler(SchedulerStore<K, P> store) {
        this.store = store;
    }

    /**
     * Schedules a key for immediate processing.
     * <ul>
     * <li>Not scheduled → insert into immediate zone</li>
     * <li>Already immediate → no-op</li>
     * <li>Currently observed → promote to immediate</li>
     * </ul>
     */
    @Override
    public void scheduleNow(K key, P payload) {
        Long existingPosition = store.getPosition(key);

        if (existingPosition != null) {
            if (isImmediate(existingPosition)) {
                return;
            }

            store.deleteEntry(existingPosition, key);
            adjustCount(DELAYED_COUNT_KEY, -1);
        }

        long immediatePosition = nextImmediatePosition();
        store.putEntry(immediatePosition, key, payload);
        store.putPosition(key, immediatePosition);
        adjustCount(IMMEDIATE_COUNT_KEY, 1);
    }

    /**
     * Schedules a key for delayed processing at the given instant.
     * <ul>
     * <li>Not scheduled → insert into delayed zone</li>
     * <li>Already immediate → no-op</li>
     * <li>Already delayed → update timestamp and payload in place semantically via
     * rewrite</li>
     * </ul>
     */
    @Override
    public void scheduleLater(K key, P payload, Instant when) {
        scheduleAtEpochSeconds(key, payload, when.getEpochSecond());
    }

    /**
     * Schedules a key for delayed processing using an epoch-second position.
     */
    private void scheduleAtEpochSeconds(K key, P payload, long epochSeconds) {
        if (epochSeconds < DELAYED_BOUNDARY) {
            throw new IllegalArgumentException(
                    "epochSeconds must be >= " + DELAYED_BOUNDARY + ", got " + epochSeconds);
        }

        Long existingPosition = store.getPosition(key);

        if (existingPosition != null) {
            if (isImmediate(existingPosition)) {
                return;
            }

            store.deleteEntry(existingPosition, key);
        } else {
            adjustCount(DELAYED_COUNT_KEY, 1);
        }

        store.putEntry(epochSeconds, key, payload);
        store.putPosition(key, epochSeconds);
    }

    /**
     * Drains up to {@code limit} currently eligible entries: all immediate entries
     * plus deferred entries scheduled strictly before
     * {@code deferredCutoffExclusive}.
     */
    @Override
    public List<ScheduledItem<K, P>> drain(int limit, Instant deferredCutoffExclusive) {
        return drainRange(FIRST_IMMEDIATE_POSITION, deferredCutoffExclusive.getEpochSecond(), limit);
    }

    /**
     * Cancels the current schedule for the key, if present.
     *
     * @return true if an entry was removed; false otherwise
     */
    @Override
    public boolean cancel(K key) {
        Long existingPosition = store.getPosition(key);

        if (existingPosition == null) {
            return false;
        }

        store.deleteEntry(existingPosition, key);
        store.deletePosition(key);
        adjustCount(isImmediate(existingPosition) ? IMMEDIATE_COUNT_KEY : DELAYED_COUNT_KEY, -1);
        resetImmediateCounterIfEmpty();
        return true;
    }

    @Override
    public boolean isScheduled(K key) {
        return store.getPosition(key) != null;
    }

    @Override
    public long sizeImmediate() {
        return store.getMetadata(IMMEDIATE_COUNT_KEY);
    }

    @Override
    public long sizeDelayed() {
        return store.getMetadata(DELAYED_COUNT_KEY);
    }

    @Override
    public long sizeTotal() {
        return sizeImmediate() + sizeDelayed();
    }

    private List<ScheduledItem<K, P>> drainRange(long fromPositionInclusive, Long toPositionExclusive, int limit) {
        List<SchedulerStore.Entry<K, P>> toDelete = new ArrayList<>(limit);
        List<ScheduledItem<K, P>> drained = new ArrayList<>(limit);
        int drainedImmediateCount = 0;
        int drainedDelayedCount = 0;

        try (SchedulerStore.EntryIterator<K, P> it = store.scanEntries(fromPositionInclusive, toPositionExclusive,
                limit)) {
            while (it.hasNext() && drained.size() < limit) {
                SchedulerStore.Entry<K, P> entry = it.next();
                drained.add(new ScheduledItem<>(entry.key(), entry.payload()));
                toDelete.add(entry);

                if (isImmediate(entry.position())) {
                    drainedImmediateCount++;
                } else {
                    drainedDelayedCount++;
                }
            }
        }

        for (SchedulerStore.Entry<K, P> entry : toDelete) {
            store.deleteEntry(entry.position(), entry.key());
            store.deletePosition(entry.key());
        }

        if (drainedImmediateCount > 0) {
            adjustCount(IMMEDIATE_COUNT_KEY, -drainedImmediateCount);
        }
        if (drainedDelayedCount > 0) {
            adjustCount(DELAYED_COUNT_KEY, -drainedDelayedCount);
        }
        if (drainedImmediateCount > 0) {
            resetImmediateCounterIfEmpty();
        }

        return drained;
    }

    private long nextImmediatePosition() {
        long nextPosition = Math.max(store.getMetadata(NEXT_IMMEDIATE_POSITION_KEY), FIRST_IMMEDIATE_POSITION);
        store.putMetadata(NEXT_IMMEDIATE_POSITION_KEY, nextPosition + 1);
        return nextPosition;
    }

    private void resetImmediateCounterIfEmpty() {
        if (sizeImmediate() == 0) {
            store.putMetadata(NEXT_IMMEDIATE_POSITION_KEY, FIRST_IMMEDIATE_POSITION);
        }
    }

    private void adjustCount(String countKey, long delta) {
        long current = store.getMetadata(countKey);
        store.putMetadata(countKey, Math.max(0L, current + delta));
    }

    static boolean isImmediate(long position) {
        return position >= FIRST_IMMEDIATE_POSITION && position < DELAYED_BOUNDARY;
    }
}
