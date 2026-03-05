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
 * Minimal byte-array key-value store interface required by the scheduler.
 *
 * <p>
 * Implementations must provide byte-ordered key iteration so that range scans
 * return entries in ascending unsigned-byte order.
 */
public interface ByteArrayKeyValueStore {

    /**
     * Returns the value associated with {@code key}, or {@code null} if absent.
     */
    byte[] get(byte[] key);

    /**
     * Associates {@code key} with {@code value}, replacing any previous mapping.
     */
    void put(byte[] key, byte[] value);

    /**
     * Removes the entry for {@code key}, if present.
     */
    void delete(byte[] key);

    /**
     * Returns an iterator over entries whose keys fall in the range
     * [{@code fromInclusive}, {@code toExclusive}), ordered by unsigned-byte
     * ascending key order. A {@code null} bound means open (no limit in that
     * direction).
     */
    RangeIterator range(byte[] fromInclusive, byte[] toExclusive);

    /**
     * An ordered, closeable iterator over {@link Entry} values.
     */
    interface RangeIterator extends AutoCloseable {

        boolean hasNext();

        Entry next();

        @Override
        void close();
    }

    /**
     * A key-value pair returned by {@link RangeIterator#next()}.
     */
    record Entry(byte[] key, byte[] value) {
    }
}
