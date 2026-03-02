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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

final class InMemoryByteArrayKeyValueStore implements KeyValueStore<byte[], byte[]> {

    private final NavigableMap<byte[], byte[]> entries = new TreeMap<>(Arrays::compareUnsigned);
    private boolean open = true;

    @Override
    public String name() {
        return "test-store";
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        open = true;
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        entries.put(copy(key), copy(value));
    }

    @Override
    public byte[] putIfAbsent(byte[] key, byte[] value) {
        byte[] existing = entries.get(key);
        if (existing != null) {
            return copy(existing);
        }

        put(key, value);
        return null;
    }

    @Override
    public void putAll(List<KeyValue<byte[], byte[]>> list) {
        for (KeyValue<byte[], byte[]> entry : list) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public byte[] delete(byte[] key) {
        return copy(entries.remove(key));
    }

    @Override
    public byte[] get(byte[] key) {
        return copy(entries.get(key));
    }

    @Override
    public KeyValueIterator<byte[], byte[]> range(byte[] from, byte[] to) {
        NavigableMap<byte[], byte[]> tail = (from == null) ? entries : entries.tailMap(from, true);
        NavigableMap<byte[], byte[]> view = (to == null) ? tail : tail.headMap(to, false);
        return snapshot(view);
    }

    @Override
    public KeyValueIterator<byte[], byte[]> all() {
        return snapshot(entries);
    }

    @Override
    public long approximateNumEntries() {
        return entries.size();
    }

    byte[] rawGet(byte[] key) {
        return get(key);
    }

    boolean containsRawKey(byte[] key) {
        return entries.containsKey(key);
    }

    private static KeyValueIterator<byte[], byte[]> snapshot(NavigableMap<byte[], byte[]> view) {
        List<KeyValue<byte[], byte[]>> items = new ArrayList<>(view.size());
        for (var entry : view.entrySet()) {
            items.add(KeyValue.pair(copy(entry.getKey()), copy(entry.getValue())));
        }
        return new SnapshotIterator(items);
    }

    private static byte[] copy(byte[] bytes) {
        return (bytes == null) ? null : Arrays.copyOf(bytes, bytes.length);
    }

    private static final class SnapshotIterator implements KeyValueIterator<byte[], byte[]> {

        private final List<KeyValue<byte[], byte[]>> items;
        private int index;

        private SnapshotIterator(List<KeyValue<byte[], byte[]>> items) {
            this.items = items;
        }

        @Override
        public boolean hasNext() {
            return index < items.size();
        }

        @Override
        public KeyValue<byte[], byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            KeyValue<byte[], byte[]> entry = items.get(index++);
            return KeyValue.pair(copy(entry.key), copy(entry.value));
        }

        @Override
        public byte[] peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return copy(items.get(index).key);
        }

        @Override
        public void close() {
        }
    }
}
