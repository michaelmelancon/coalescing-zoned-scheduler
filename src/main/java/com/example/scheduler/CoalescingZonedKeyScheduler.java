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

import java.time.Instant;
import java.util.List;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Key-only facade over {@link CoalescingZonedScheduler} for workloads where scheduling tracks
 * eligibility only and the authoritative value is read from elsewhere at processing time.
 */
public class CoalescingZonedKeyScheduler<K> {

    private static final byte[] MARKER = new byte[] {1};
    private final CoalescingZonedScheduler<K, byte[]> delegate;

    public CoalescingZonedKeyScheduler(KeyValueStore<byte[], byte[]> store, Serde<K> keySerde) {
        this.delegate = new CoalescingZonedScheduler<>(store, keySerde, Serdes.ByteArray());
    }

    public void scheduleNow(K key) {
        delegate.scheduleNow(key, MARKER);
    }

    public void scheduleAt(K key, Instant when) {
        delegate.scheduleAt(key, MARKER, when);
    }

    public boolean cancel(K key) {
        return delegate.cancel(key);
    }

    public boolean isScheduled(K key) {
        return delegate.isScheduled(key);
    }

    public List<K> drainReady(int limit, Instant observedCutoffExclusive) {
        return delegate.drainReady(limit, observedCutoffExclusive)
                .stream()
                .map(ScheduledItem::key)
                .toList();
    }

    public List<K> drainImmediate(int limit) {
        return delegate.drainImmediate(limit)
                .stream()
                .map(ScheduledItem::key)
                .toList();
    }

    public long sizeNow() {
        return delegate.sizeNow();
    }

    public long sizeObserved() {
        return delegate.sizeObserved();
    }

    public long sizeTotal() {
        return delegate.sizeTotal();
    }
}
