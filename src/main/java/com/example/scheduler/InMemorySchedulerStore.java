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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * In-memory implementation of {@link SchedulerStore} suitable for testing and
 * single-process workloads. No serialization is performed; keys and payloads
 * are stored by reference.
 *
 * <p>
 * Entries at the same zone position are iterated in insertion order.
 *
 * @param <K> logical key type
 * @param <P> payload type
 */
public class InMemorySchedulerStore<K, P> implements SchedulerStore<K, P> {

    private final Map<K, Long> positionIndex = new HashMap<>();
    private final TreeMap<Long, LinkedHashMap<K, P>> entries = new TreeMap<>();
    private final Map<String, Long> metadata = new HashMap<>();

    @Override
    public Long getPosition(K key) {
        return positionIndex.get(key);
    }

    @Override
    public void putPosition(K key, long position) {
        positionIndex.put(key, position);
    }

    @Override
    public void deletePosition(K key) {
        positionIndex.remove(key);
    }

    @Override
    public void putEntry(long position, K key, P payload) {
        entries.computeIfAbsent(position, k -> new LinkedHashMap<>()).put(key, payload);
    }

    @Override
    public void deleteEntry(long position, K key) {
        LinkedHashMap<K, P> atPosition = entries.get(position);
        if (atPosition != null) {
            atPosition.remove(key);
            if (atPosition.isEmpty()) {
                entries.remove(position);
            }
        }
    }

    @Override
    public EntryIterator<K, P> scanEntries(long fromPositionInclusive, Long toPositionExclusive, int maxResults) {
        NavigableMap<Long, LinkedHashMap<K, P>> range = (toPositionExclusive == null)
                ? entries.tailMap(fromPositionInclusive, true)
                : entries.subMap(fromPositionInclusive, true, toPositionExclusive, false);

        List<Entry<K, P>> snapshot = new ArrayList<>();
        for (var posEntry : range.entrySet()) {
            long pos = posEntry.getKey();
            for (var kv : posEntry.getValue().entrySet()) {
                if (snapshot.size() >= maxResults) {
                    break;
                }
                snapshot.add(new Entry<>(pos, kv.getKey(), kv.getValue()));
            }
            if (snapshot.size() >= maxResults) {
                break;
            }
        }
        return new SnapshotEntryIterator<>(snapshot);
    }

    @Override
    public long getMetadata(String key) {
        return metadata.getOrDefault(key, 0L);
    }

    @Override
    public void putMetadata(String key, long value) {
        metadata.put(key, value);
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
