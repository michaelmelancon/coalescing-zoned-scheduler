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
import java.util.List;

/**
 * Key-only facade over {@link CoalescingZonedScheduler} for workloads
 * where scheduling tracks eligibility only and the authoritative value is read
 * from elsewhere at processing time.
 */
public class CoalescingZonedKeyScheduler<K> {

    private static final byte[] MARKER = new byte[] { 1 };
    private final CoalescingZonedScheduler<K, byte[]> delegate;

    public CoalescingZonedKeyScheduler(CoalescingZonedScheduler<K, byte[]> scheduler) {
        this.delegate = scheduler;
    }

    public void scheduleNow(K key) {
        delegate.scheduleNow(key, MARKER);
    }

    public void scheduleLater(K key, Instant when) {
        delegate.scheduleLater(key, MARKER, when);
    }

    public boolean cancel(K key) {
        return delegate.cancel(key);
    }

    public boolean isScheduled(K key) {
        return delegate.isScheduled(key);
    }

    public List<K> drain(int limit, Instant delayedCutoffExclusive) {
        return delegate.drain(limit, delayedCutoffExclusive)
                .stream()
                .map(ScheduledItem::key)
                .toList();
    }

    public List<K> drain(int limit) {
        return drain(limit, Instant.now());
    }

    public long sizeImmediate() {
        return delegate.sizeImmediate();
    }

    public long sizeDelayed() {
        return delegate.sizeDelayed();
    }

    public long sizeTotal() {
        return delegate.sizeTotal();
    }
}
