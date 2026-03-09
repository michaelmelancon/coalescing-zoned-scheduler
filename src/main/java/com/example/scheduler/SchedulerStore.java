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

package com.example.scheduler;

/**
 * Backing store contract for {@link KeyValueStoreCoalescingZonedScheduler}.
 *
 * <p>
 * The interface is intentionally minimal and generic: implementations are
 * responsible for any serialization required to persist keys of type {@code K}
 * and payloads of type {@code P}. The scheduler itself is never concerned with
 * how values are encoded.
 *
 * @param <K> logical key type
 * @param <P> payload type
 */
public interface SchedulerStore<K, P> {

    /**
     * Returns the current zone position recorded for {@code key}, or
     * {@code null} if the key is not scheduled.
     */
    Long getPosition(K key);

    /**
     * Associates {@code key} with the given zone {@code position}.
     */
    void putPosition(K key, long position);

    /**
     * Removes the zone position for {@code key}, if present.
     */
    void deletePosition(K key);

    /**
     * Stores {@code payload} for the given {@code (position, key)} pair.
     */
    void putEntry(long position, K key, P payload);

    /**
     * Removes the entry for the given {@code (position, key)} pair, if present.
     */
    void deleteEntry(long position, K key);

    /**
     * Returns an iterator over scheduled entries whose zone position falls in
     * [{@code fromPositionInclusive}, {@code toPositionExclusive}), ordered by
     * ascending position. A {@code null} upper bound means no limit.
     *
     * @param maxResults maximum number of entries the iterator will return
     */
    EntryIterator<K, P> scanEntries(long fromPositionInclusive, Long toPositionExclusive, int maxResults);

    /**
     * Returns the long value stored under {@code key}, or {@code 0} if absent.
     */
    long getMetadata(String key);

    /**
     * Stores {@code value} under the metadata {@code key}.
     */
    void putMetadata(String key, long value);

    /**
     * A closeable, ordered iterator over {@link Entry} values produced by
     * {@link #scanEntries}.
     */
    interface EntryIterator<K, P> extends AutoCloseable {

        boolean hasNext();

        Entry<K, P> next();

        @Override
        void close();
    }

    /**
     * A single scheduled entry returned by {@link EntryIterator#next()}.
     */
    record Entry<K, P>(long position, K key, P payload) {
    }
}
