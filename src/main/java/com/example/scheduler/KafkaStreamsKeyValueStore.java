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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * Adapts a Kafka Streams {@link org.apache.kafka.streams.state.KeyValueStore
 * KeyValueStore}{@code <byte[], byte[]>} to the {@link ByteArrayKeyValueStore}
 * interface, allowing the scheduler to work with Kafka Streams state stores
 * without a direct dependency on the Kafka Streams API.
 */
public class KafkaStreamsKeyValueStore implements ByteArrayKeyValueStore {

    private final org.apache.kafka.streams.state.KeyValueStore<byte[], byte[]> delegate;

    public KafkaStreamsKeyValueStore(
            org.apache.kafka.streams.state.KeyValueStore<byte[], byte[]> delegate) {
        this.delegate = delegate;
    }

    @Override
    public byte[] get(byte[] key) {
        return delegate.get(key);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        delegate.put(key, value);
    }

    @Override
    public void delete(byte[] key) {
        delegate.delete(key);
    }

    @Override
    public RangeIterator range(byte[] fromInclusive, byte[] toExclusive) {
        return new KafkaRangeIterator(delegate.range(fromInclusive, toExclusive));
    }

    private static final class KafkaRangeIterator implements RangeIterator {

        private final KeyValueIterator<byte[], byte[]> delegate;

        KafkaRangeIterator(KeyValueIterator<byte[], byte[]> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Entry next() {
            KeyValue<byte[], byte[]> kv = delegate.next();
            return new Entry(kv.key, kv.value);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
