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

/**
 * In-memory implementation of {@link ByteArrayKeyValueStore} backed by an
 * unsigned-byte-sorted {@link TreeMap}.
 */
public class InMemoryKeyValueStore implements ByteArrayKeyValueStore {

    private final NavigableMap<byte[], byte[]> entries = new TreeMap<>(Arrays::compareUnsigned);

    @Override
    public byte[] get(byte[] key) {
        return copy(entries.get(key));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        entries.put(copy(key), copy(value));
    }

    @Override
    public void delete(byte[] key) {
        entries.remove(key);
    }

    @Override
    public RangeIterator range(byte[] fromInclusive, byte[] toExclusive) {
        NavigableMap<byte[], byte[]> tail = (fromInclusive == null) ? entries : entries.tailMap(fromInclusive, true);
        NavigableMap<byte[], byte[]> view = (toExclusive == null) ? tail : tail.headMap(toExclusive, false);
        return snapshot(view);
    }

    private static RangeIterator snapshot(NavigableMap<byte[], byte[]> view) {
        List<Entry> items = new ArrayList<>(view.size());
        for (var e : view.entrySet()) {
            items.add(new Entry(copy(e.getKey()), copy(e.getValue())));
        }
        return new SnapshotRangeIterator(items);
    }

    private static byte[] copy(byte[] bytes) {
        return (bytes == null) ? null : Arrays.copyOf(bytes, bytes.length);
    }

    private static final class SnapshotRangeIterator implements RangeIterator {

        private final List<Entry> items;
        private int index;

        SnapshotRangeIterator(List<Entry> items) {
            this.items = items;
        }

        @Override
        public boolean hasNext() {
            return index < items.size();
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return items.get(index++);
        }

        @Override
        public void close() {
        }
    }
}
