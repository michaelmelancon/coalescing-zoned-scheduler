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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * Kafka Streams implementation of {@link SchedulerStore} backed by a single
 * {@link org.apache.kafka.streams.state.KeyValueStore}{@code <byte[], byte[]>}.
 *
 * <p>
 * All serialization and byte-level key encoding is encapsulated here, keeping
 * the scheduler free of Kafka-specific concerns.
 *
 * <h2>Key-space layout</h2>
 * <table>
 * <caption>Zone layout within the backing store</caption>
 * <tr>
 * <th>Zone</th>
 * <th>Prefix (first 8 bytes)</th>
 * <th>Purpose</th>
 * </tr>
 * <tr>
 * <td>Position index</td>
 * <td>0</td>
 * <td>Logical key → current zone position</td>
 * </tr>
 * <tr>
 * <td>Metadata</td>
 * <td>1</td>
 * <td>Named long counters</td>
 * </tr>
 * <tr>
 * <td>Scheduled entries</td>
 * <td>[2, ∞)</td>
 * <td>Position-ordered (immediate then delayed) work items</td>
 * </tr>
 * </table>
 *
 * @param <K> logical key type
 * @param <P> payload type
 */
public class KafkaStreamsSchedulerStore<K, P> implements SchedulerStore<K, P> {

    static final long POSITION_INDEX_PREFIX = 0L;
    static final long METADATA_PREFIX = 1L;

    private static final byte DELIMITER = 0x00;

    private final org.apache.kafka.streams.state.KeyValueStore<byte[], byte[]> delegate;
    private final Serializer<K> keySerializer;
    private final Deserializer<K> keyDeserializer;
    private final Serializer<P> payloadSerializer;
    private final Deserializer<P> payloadDeserializer;

    public KafkaStreamsSchedulerStore(
            org.apache.kafka.streams.state.KeyValueStore<byte[], byte[]> delegate,
            Serde<K> keySerde,
            Serde<P> payloadSerde) {
        this.delegate = delegate;
        this.keySerializer = keySerde.serializer();
        this.keyDeserializer = keySerde.deserializer();
        this.payloadSerializer = payloadSerde.serializer();
        this.payloadDeserializer = payloadSerde.deserializer();
    }

    @Override
    public Long getPosition(K key) {
        byte[] raw = delegate.get(positionIndexKey(key));
        return (raw != null) ? decodeLong(raw) : null;
    }

    @Override
    public void putPosition(K key, long position) {
        delegate.put(positionIndexKey(key), encodeLong(position));
    }

    @Override
    public void deletePosition(K key) {
        delegate.delete(positionIndexKey(key));
    }

    @Override
    public void putEntry(long position, K key, P payload) {
        delegate.put(compositeKey(position, keySerializer.serialize(null, key)),
                payloadSerializer.serialize(null, payload));
    }

    @Override
    public void deleteEntry(long position, K key) {
        delegate.delete(compositeKey(position, keySerializer.serialize(null, key)));
    }

    @Override
    public EntryIterator<K, P> scanEntries(long fromPositionInclusive, Long toPositionExclusive, int maxResults) {
        byte[] fromKey = rangeStartKey(fromPositionInclusive);
        byte[] toKey = (toPositionExclusive == null) ? null : rangeStartKey(toPositionExclusive);

        List<Entry<K, P>> snapshot = new ArrayList<>();
        try (KeyValueIterator<byte[], byte[]> it = delegate.range(fromKey, toKey)) {
            while (it.hasNext() && snapshot.size() < maxResults) {
                KeyValue<byte[], byte[]> kv = it.next();
                byte[] logicalKeyBytes = extractKeyBytes(kv.key);
                if (logicalKeyBytes.length == 0) {
                    continue;
                }

                K k = keyDeserializer.deserialize(null, logicalKeyBytes);
                if (k == null) {
                    continue;
                }

                P payload = payloadDeserializer.deserialize(null, kv.value);
                snapshot.add(new Entry<>(extractPrefix(kv.key), k, payload));
            }
        }
        return new SnapshotEntryIterator<>(snapshot);
    }

    @Override
    public long getMetadata(String key) {
        byte[] raw = delegate.get(metadataKey(key));
        return (raw != null) ? decodeLong(raw) : 0L;
    }

    @Override
    public void putMetadata(String key, long value) {
        delegate.put(metadataKey(key), encodeLong(value));
    }

    // -------------------------------------------------------------------------
    // Byte-encoding helpers (package-private for testing)
    // -------------------------------------------------------------------------

    static byte[] compositeKey(long prefix, byte[] logicalKeyBytes) {
        ByteBuffer buf = ByteBuffer.allocate(8 + 1 + logicalKeyBytes.length);
        buf.putLong(prefix);
        buf.put(DELIMITER);
        buf.put(logicalKeyBytes);
        return buf.array();
    }

    static byte[] rangeStartKey(long prefix) {
        ByteBuffer buf = ByteBuffer.allocate(9);
        buf.putLong(prefix);
        buf.put(DELIMITER);
        return buf.array();
    }

    static byte[] encodeLong(long value) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(value);
        return buf.array();
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

    private byte[] positionIndexKey(K key) {
        return compositeKey(POSITION_INDEX_PREFIX, keySerializer.serialize(null, key));
    }

    private static byte[] metadataKey(String name) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(8 + 1 + nameBytes.length);
        buf.putLong(METADATA_PREFIX);
        buf.put(DELIMITER);
        buf.put(nameBytes);
        return buf.array();
    }

    private static final class SnapshotEntryIterator<K, P> implements EntryIterator<K, P> {

        private final List<Entry<K, P>> items;
        private int index;

        SnapshotEntryIterator(List<Entry<K, P>> items) {
            this.items = items;
        }

        @Override
        public boolean hasNext() {
            return index < items.size();
        }

        @Override
        public Entry<K, P> next() {
            return items.get(index++);
        }

        @Override
        public void close() {
        }
    }
}
