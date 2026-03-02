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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * A persistent keyed scheduler backed by a single
 * {@link KeyValueStore}{@code <byte[], byte[]>}
 * with multiple logical zones, leveraging byte-ordered keys for efficient
 * draining.
 *
 * <p>
 * The scheduler maintains at most one live scheduled entry per logical key.
 * Immediate entries
 * drain before observed entries. Re-scheduling coalesces by key rather than
 * preserving every
 * enqueue.
 *
 * <h2>Zone layout</h2>
 * <table>
 * <tr>
 * <th>Zone</th>
 * <th>Prefix range</th>
 * <th>Purpose</th>
 * </tr>
 * <tr>
 * <td>Reverse lookup</td>
 * <td>0</td>
 * <td>Logical key → current scheduled position</td>
 * </tr>
 * <tr>
 * <td>Metadata</td>
 * <td>1</td>
 * <td>Immediate counter + per-zone counts</td>
 * </tr>
 * <tr>
 * <td>Immediate</td>
 * <td>[2, 1B)</td>
 * <td>FIFO-like ordering using a recyclable counter</td>
 * </tr>
 * <tr>
 * <td>Observed</td>
 * <td>[1B, ∞)</td>
 * <td>Time-ordered scheduling using epoch seconds</td>
 * </tr>
 * </table>
 *
 * <p>
 * This payload-carrying form is appropriate only when coalescing by logical key
 * is correct.
 * If the same key must preserve multiple outstanding work items independently,
 * this is the wrong
 * abstraction.
 */
public class CoalescingZonedScheduler<K, P> {

    static final long REVERSE_LOOKUP_PREFIX = 0L;
    static final long METADATA_PREFIX = 1L;
    static final long FIRST_IMMEDIATE_POSITION = 2L;
    static final long OBSERVED_BOUNDARY = 1_000_000_000L;

    private static final byte DELIMITER = 0x00;

    private static final byte[] NEXT_IMMEDIATE_POSITION_KEY = metadataKey("next-immediate-position");
    private static final byte[] IMMEDIATE_COUNT_KEY = metadataKey("immediate-count");
    private static final byte[] OBSERVED_COUNT_KEY = metadataKey("observed-count");

    private final KeyValueStore<byte[], byte[]> store;
    private final Serializer<K> keySerializer;
    private final Deserializer<K> keyDeserializer;
    private final Serializer<P> payloadSerializer;
    private final Deserializer<P> payloadDeserializer;

    public CoalescingZonedScheduler(
            KeyValueStore<byte[], byte[]> store,
            Serde<K> keySerde,
            Serde<P> payloadSerde) {
        this.store = store;
        this.keySerializer = keySerde.serializer();
        this.keyDeserializer = keySerde.deserializer();
        this.payloadSerializer = payloadSerde.serializer();
        this.payloadDeserializer = payloadSerde.deserializer();
    }

    /**
     * Schedules a key for immediate processing.
     * <ul>
     * <li>Not scheduled → insert into immediate zone</li>
     * <li>Already immediate → no-op</li>
     * <li>Currently observed → promote to immediate</li>
     * </ul>
     */
    public void scheduleNow(K key, P payload) {
        byte[] keyBytes = serializeKey(key);
        byte[] lookupKey = reverseLookupKey(keyBytes);
        byte[] existingPositionBytes = store.get(lookupKey);

        if (existingPositionBytes != null) {
            long existingPosition = decodeLong(existingPositionBytes);
            if (isImmediate(existingPosition)) {
                return;
            }

            store.delete(compositeKey(existingPosition, keyBytes));
            adjustCount(OBSERVED_COUNT_KEY, -1);
        }

        long immediatePosition = nextImmediatePosition();
        store.put(compositeKey(immediatePosition, keyBytes), serializePayload(payload));
        store.put(lookupKey, encodeLong(immediatePosition));
        adjustCount(IMMEDIATE_COUNT_KEY, 1);
    }

    /**
     * Schedules a key for delayed processing at the given instant.
     * <ul>
     * <li>Not scheduled → insert into observed zone</li>
     * <li>Already immediate → no-op</li>
     * <li>Already observed → update timestamp and payload in place semantically via
     * rewrite</li>
     * </ul>
     */
    public void scheduleAt(K key, P payload, Instant when) {
        scheduleAtEpochSeconds(key, payload, when.getEpochSecond());
    }

    /**
     * Schedules a key for delayed processing using an epoch-second position.
     */
    public void scheduleAtEpochSeconds(K key, P payload, long epochSeconds) {
        if (epochSeconds < OBSERVED_BOUNDARY) {
            throw new IllegalArgumentException(
                    "epochSeconds must be >= " + OBSERVED_BOUNDARY + ", got " + epochSeconds);
        }

        byte[] keyBytes = serializeKey(key);
        byte[] lookupKey = reverseLookupKey(keyBytes);
        byte[] existingPositionBytes = store.get(lookupKey);

        if (existingPositionBytes != null) {
            long existingPosition = decodeLong(existingPositionBytes);
            if (isImmediate(existingPosition)) {
                return;
            }

            store.delete(compositeKey(existingPosition, keyBytes));
        } else {
            adjustCount(OBSERVED_COUNT_KEY, 1);
        }

        store.put(compositeKey(epochSeconds, keyBytes), serializePayload(payload));
        store.put(lookupKey, encodeLong(epochSeconds));
    }

    /**
     * Drains up to {@code limit} currently eligible entries: all immediate entries
     * plus observed
     * entries scheduled strictly before {@code observedCutoffExclusive}.
     */
    public List<ScheduledItem<K, P>> drainReady(int limit, Instant observedCutoffExclusive) {
        return drainRange(rangeStartKey(FIRST_IMMEDIATE_POSITION),
                rangeStartKey(observedCutoffExclusive.getEpochSecond()), limit);
    }

    /**
     * Drains up to {@code limit} immediate entries only.
     */
    public List<ScheduledItem<K, P>> drainImmediate(int limit) {
        return drainRange(rangeStartKey(FIRST_IMMEDIATE_POSITION), rangeStartKey(OBSERVED_BOUNDARY), limit);
    }

    /**
     * Cancels the current schedule for the key, if present.
     *
     * @return true if an entry was removed; false otherwise
     */
    public boolean cancel(K key) {
        byte[] keyBytes = serializeKey(key);
        byte[] lookupKey = reverseLookupKey(keyBytes);
        byte[] existingPositionBytes = store.get(lookupKey);

        if (existingPositionBytes == null) {
            return false;
        }

        long existingPosition = decodeLong(existingPositionBytes);
        store.delete(compositeKey(existingPosition, keyBytes));
        store.delete(lookupKey);
        adjustCount(isImmediate(existingPosition) ? IMMEDIATE_COUNT_KEY : OBSERVED_COUNT_KEY, -1);
        resetImmediateCounterIfEmpty();
        return true;
    }

    public boolean isScheduled(K key) {
        return store.get(reverseLookupKey(serializeKey(key))) != null;
    }

    public long sizeNow() {
        return readCount(IMMEDIATE_COUNT_KEY);
    }

    public long sizeObserved() {
        return readCount(OBSERVED_COUNT_KEY);
    }

    public long sizeTotal() {
        return sizeNow() + sizeObserved();
    }

    private List<ScheduledItem<K, P>> drainRange(byte[] fromInclusive, byte[] toExclusive, int limit) {
        List<byte[]> queueKeysToDelete = new ArrayList<>(limit);
        List<ScheduledItem<K, P>> drainedItems = new ArrayList<>(limit);
        int drainedImmediateCount = 0;
        int drainedObservedCount = 0;

        try (KeyValueIterator<byte[], byte[]> iterator = (toExclusive != null) ? store.range(fromInclusive, toExclusive)
                : store.range(fromInclusive, null)) {
            while (iterator.hasNext() && drainedItems.size() < limit) {
                var entry = iterator.next();
                byte[] logicalKeyBytes = extractKeyBytes(entry.key);
                if (logicalKeyBytes.length == 0) {
                    continue;
                }

                K key = keyDeserializer.deserialize(null, logicalKeyBytes);
                if (key == null) {
                    continue;
                }

                P payload = payloadDeserializer.deserialize(null, entry.value);
                drainedItems.add(new ScheduledItem<>(key, payload));
                queueKeysToDelete.add(entry.key);

                if (isImmediate(extractPrefix(entry.key))) {
                    drainedImmediateCount++;
                } else {
                    drainedObservedCount++;
                }
            }
        }

        for (byte[] queueKey : queueKeysToDelete) {
            store.delete(queueKey);
            store.delete(reverseLookupKey(extractKeyBytes(queueKey)));
        }

        if (drainedImmediateCount > 0) {
            adjustCount(IMMEDIATE_COUNT_KEY, -drainedImmediateCount);
        }
        if (drainedObservedCount > 0) {
            adjustCount(OBSERVED_COUNT_KEY, -drainedObservedCount);
        }
        if (drainedImmediateCount > 0) {
            resetImmediateCounterIfEmpty();
        }

        return drainedItems;
    }

    private long nextImmediatePosition() {
        byte[] raw = store.get(NEXT_IMMEDIATE_POSITION_KEY);
        long nextPosition = (raw != null) ? decodeLong(raw) : FIRST_IMMEDIATE_POSITION;
        store.put(NEXT_IMMEDIATE_POSITION_KEY, encodeLong(nextPosition + 1));
        return nextPosition;
    }

    private void resetImmediateCounterIfEmpty() {
        if (sizeNow() == 0) {
            store.put(NEXT_IMMEDIATE_POSITION_KEY, encodeLong(FIRST_IMMEDIATE_POSITION));
        }
    }

    private void adjustCount(byte[] countKey, long delta) {
        long current = readCount(countKey);
        store.put(countKey, encodeLong(Math.max(0L, current + delta)));
    }

    private long readCount(byte[] countKey) {
        byte[] raw = store.get(countKey);
        return (raw != null) ? decodeLong(raw) : 0L;
    }

    private byte[] serializeKey(K key) {
        return keySerializer.serialize(null, key);
    }

    private byte[] serializePayload(P payload) {
        return payloadSerializer.serialize(null, payload);
    }

    static boolean isImmediate(long position) {
        return position >= FIRST_IMMEDIATE_POSITION && position < OBSERVED_BOUNDARY;
    }

    static byte[] reverseLookupKey(byte[] logicalKeyBytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 1 + logicalKeyBytes.length);
        buffer.putLong(REVERSE_LOOKUP_PREFIX);
        buffer.put(DELIMITER);
        buffer.put(logicalKeyBytes);
        return buffer.array();
    }

    static byte[] compositeKey(long prefix, byte[] logicalKeyBytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 1 + logicalKeyBytes.length);
        buffer.putLong(prefix);
        buffer.put(DELIMITER);
        buffer.put(logicalKeyBytes);
        return buffer.array();
    }

    static byte[] rangeStartKey(long prefix) {
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.putLong(prefix);
        buffer.put(DELIMITER);
        return buffer.array();
    }

    private static byte[] metadataKey(String suffix) {
        byte[] suffixBytes = suffix.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(8 + 1 + suffixBytes.length);
        buffer.putLong(METADATA_PREFIX);
        buffer.put(DELIMITER);
        buffer.put(suffixBytes);
        return buffer.array();
    }

    static byte[] encodeLong(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    static long decodeLong(byte[] value) {
        if (value == null || value.length < Long.BYTES) {
            return 0L;
        }
        return ByteBuffer.wrap(value).getLong();
    }

    static long extractPrefix(byte[] compositeKey) {
        if (compositeKey == null || compositeKey.length < Long.BYTES) {
            return 0L;
        }
        return ByteBuffer.wrap(compositeKey, 0, Long.BYTES).getLong();
    }

    static byte[] extractKeyBytes(byte[] compositeKey) {
        if (compositeKey == null || compositeKey.length <= 9) {
            return new byte[0];
        }
        byte[] logicalKeyBytes = new byte[compositeKey.length - 9];
        System.arraycopy(compositeKey, 9, logicalKeyBytes, 0, logicalKeyBytes.length);
        return logicalKeyBytes;
    }
}
