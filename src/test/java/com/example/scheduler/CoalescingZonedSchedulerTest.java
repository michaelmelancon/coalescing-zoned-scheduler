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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CoalescingZonedSchedulerTest {

    private InMemoryByteArrayKeyValueStore store;
    private KeyValueStoreCoalescingZonedScheduler<String, String> scheduler;

    @BeforeEach
    void setUp() {
        store = new InMemoryByteArrayKeyValueStore();
        scheduler = new KeyValueStoreCoalescingZonedScheduler<>(store, Serdes.String(), Serdes.String());
    }

    @Test
    void scheduleNowAddsImmediateEntryAndTracksCounts() {
        scheduler.scheduleNow("alpha", "payload-1");

        assertTrue(scheduler.isScheduled("alpha"));
        assertEquals(1, scheduler.sizeImmediate());
        assertEquals(0, scheduler.sizeDelayed());
        assertEquals(1, scheduler.sizeTotal());
        assertEquals(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, lookupPosition("alpha"));

        List<ScheduledItem<String, String>> drained = scheduler.drain(10);

        assertEquals(List.of("alpha"), drainedKeys(drained));
        assertEquals(List.of("payload-1"), drainedPayloads(drained));
        assertFalse(scheduler.isScheduled("alpha"));
        assertEquals(0, scheduler.sizeImmediate());
        assertEquals(0, scheduler.sizeDelayed());
        assertEquals(0, scheduler.sizeTotal());
    }

    @Test
    void scheduleNowDoesNothingWhenKeyIsAlreadyImmediate() {
        scheduler.scheduleNow("alpha", "first");
        scheduler.scheduleNow("alpha", "second");

        assertEquals(1, scheduler.sizeImmediate());
        assertEquals(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, lookupPosition("alpha"));

        List<ScheduledItem<String, String>> drained = scheduler.drain(10);

        assertEquals(List.of("alpha"), drainedKeys(drained));
        assertEquals(List.of("first"), drainedPayloads(drained));
    }

    @Test
    void scheduleNowPromotesObservedEntriesToImmediate() {
        long delayedPosition = KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY + 10;
        scheduler.scheduleLater("alpha", "delayed", Instant.ofEpochSecond(delayedPosition));

        scheduler.scheduleNow("alpha", "promoted");

        assertEquals(1, scheduler.sizeImmediate());
        assertEquals(0, scheduler.sizeDelayed());
        assertFalse(store.containsRawKey(
                KeyValueStoreCoalescingZonedScheduler.compositeKey(delayedPosition, keyBytes("alpha"))));

        List<ScheduledItem<String, String>> drained = scheduler.drain(10, observedAt(20));

        assertEquals(List.of("alpha"), drainedKeys(drained));
        assertEquals(List.of("promoted"), drainedPayloads(drained));
    }

    @Test
    void scheduleAtAddsObservedEntriesAndRewritesObservedSchedules() {
        scheduler.scheduleLater("alpha", "first", observedAt(10));
        scheduler.scheduleLater("alpha", "second", observedAt(20));

        assertEquals(0, scheduler.sizeImmediate());
        assertEquals(1, scheduler.sizeDelayed());
        assertEquals(1, scheduler.sizeTotal());
        assertTrue(scheduler.drain(10, observedAt(15)).isEmpty());

        List<ScheduledItem<String, String>> drained = scheduler.drain(10, observedAt(21));

        assertEquals(List.of("alpha"), drainedKeys(drained));
        assertEquals(List.of("second"), drainedPayloads(drained));
        assertEquals(0, scheduler.sizeDelayed());
    }

    @Test
    void scheduleAtDoesNothingWhenKeyIsAlreadyImmediate() {
        scheduler.scheduleNow("alpha", "now");

        scheduler.scheduleLater("alpha", "later", observedAt(10));

        assertEquals(1, scheduler.sizeImmediate());
        assertEquals(0, scheduler.sizeDelayed());
        assertEquals(List.of("now"), drainedPayloads(scheduler.drain(10)));
    }

    @Test
    void scheduleLaterRejectsPositionsBeforeObservedBoundary() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> scheduler.scheduleLater("alpha", "payload",
                        Instant.ofEpochSecond(KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY - 1)));

        assertTrue(thrown.getMessage().contains("epochSeconds must be >="));
    }

    @Test
    void drainReadyPrioritizesImmediateThenEligibleObservedAndHonorsLimit() {
        scheduler.scheduleLater("delayed-1", "observed-1", observedAt(10));
        scheduler.scheduleNow("immediate-1", "now-1");
        scheduler.scheduleLater("delayed-2", "observed-2", observedAt(20));
        scheduler.scheduleNow("immediate-2", "now-2");

        List<ScheduledItem<String, String>> firstBatch = scheduler.drain(3, observedAt(15));

        assertEquals(List.of("immediate-1", "immediate-2", "delayed-1"), drainedKeys(firstBatch));
        assertEquals(List.of("now-1", "now-2", "observed-1"), drainedPayloads(firstBatch));
        assertEquals(0, scheduler.sizeImmediate());
        assertEquals(1, scheduler.sizeDelayed());

        List<ScheduledItem<String, String>> secondBatch = scheduler.drain(10, observedAt(25));

        assertEquals(List.of("delayed-2"), drainedKeys(secondBatch));
        assertEquals(List.of("observed-2"), drainedPayloads(secondBatch));
        assertEquals(0, scheduler.sizeTotal());
    }

    @Test
    void drainReadyWithZeroLimitLeavesQueuedEntriesUntouched() {
        scheduler.scheduleNow("alpha", "now");

        List<ScheduledItem<String, String>> drained = scheduler.drain(0, observedAt(10));

        assertTrue(drained.isEmpty());
        assertTrue(scheduler.isScheduled("alpha"));
        assertEquals(1, scheduler.sizeImmediate());
        assertEquals(0, scheduler.sizeDelayed());
    }

    @Test
    void cancelRemovesEntriesAndResetsImmediateCounterWhenEmpty() {
        assertFalse(scheduler.cancel("missing"));

        scheduler.scheduleNow("alpha", "now");
        assertTrue(scheduler.cancel("alpha"));
        assertEquals(0, scheduler.sizeImmediate());

        scheduler.scheduleNow("beta", "next");
        assertEquals(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, lookupPosition("beta"));

        scheduler.scheduleLater("gamma", "later", observedAt(30));
        assertTrue(scheduler.cancel("gamma"));
        assertFalse(scheduler.cancel("gamma"));
        assertEquals(1, scheduler.sizeImmediate());
        assertEquals(0, scheduler.sizeDelayed());
    }

    @Test
    void drainReadySkipsMalformedEntriesAndNullDeserializedKeys() {
        Serde<String> nullableKeySerde = Serdes.serdeFrom(
                (Serializer<String>) (topic, data) -> (data == null) ? null : data.getBytes(StandardCharsets.UTF_8),
                (Deserializer<String>) (topic, data) -> {
                    if (data == null) {
                        return null;
                    }

                    String value = new String(data, StandardCharsets.UTF_8);
                    return value.equals("null-key") ? null : value;
                });
        CoalescingZonedScheduler<String, String> nullableScheduler = new KeyValueStoreCoalescingZonedScheduler<>(
                store,
                nullableKeySerde, Serdes.String());

        byte[] malformedKey = KeyValueStoreCoalescingZonedScheduler
                .rangeStartKey(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION);
        byte[] nullKey = KeyValueStoreCoalescingZonedScheduler.compositeKey(
                KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION + 1,
                keyBytes("null-key"));
        byte[] validKey = KeyValueStoreCoalescingZonedScheduler.compositeKey(
                KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION + 2,
                keyBytes("valid"));

        store.put(malformedKey, keyBytes("ignored"));
        store.put(nullKey, keyBytes("ignored-null"));
        store.put(validKey, keyBytes("payload"));

        List<ScheduledItem<String, String>> drained = nullableScheduler.drain(10);

        assertEquals(List.of("valid"), drainedKeys(drained));
        assertEquals(List.of("payload"), drainedPayloads(drained));
        assertTrue(store.containsRawKey(malformedKey));
        assertTrue(store.containsRawKey(nullKey));
        assertFalse(store.containsRawKey(validKey));
    }

    @Test
    void privateDrainRangeSupportsOpenEndedUpperBound() throws Exception {
        scheduler.scheduleNow("immediate", "now");
        scheduler.scheduleLater("observed", "later", observedAt(5));

        List<ScheduledItem<String, String>> drained = invokeDrainRange(
                KeyValueStoreCoalescingZonedScheduler
                        .rangeStartKey(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION),
                null,
                10);

        assertEquals(List.of("immediate", "observed"), drainedKeys(drained));
        assertEquals(List.of("now", "later"), drainedPayloads(drained));
        assertEquals(0, scheduler.sizeTotal());
    }

    @Test
    void staticHelpersEncodeAndExtractValues() {
        byte[] logicalKey = keyBytes("alpha");
        byte[] compositeKey = KeyValueStoreCoalescingZonedScheduler.compositeKey(42L, logicalKey);

        assertTrue(KeyValueStoreCoalescingZonedScheduler
                .isImmediate(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION));
        assertTrue(
                KeyValueStoreCoalescingZonedScheduler
                        .isImmediate(KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY - 1));
        assertFalse(KeyValueStoreCoalescingZonedScheduler
                .isImmediate(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION - 1));
        assertFalse(KeyValueStoreCoalescingZonedScheduler
                .isImmediate(KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY));

        assertEquals(42L,
                KeyValueStoreCoalescingZonedScheduler
                        .decodeLong(KeyValueStoreCoalescingZonedScheduler.encodeLong(42L)));
        assertEquals(0L, KeyValueStoreCoalescingZonedScheduler.decodeLong(null));
        assertEquals(0L, KeyValueStoreCoalescingZonedScheduler.decodeLong(new byte[] { 1, 2, 3 }));

        assertArrayEquals(logicalKey, KeyValueStoreCoalescingZonedScheduler.extractKeyBytes(compositeKey));
        assertEquals(42L, KeyValueStoreCoalescingZonedScheduler.extractPrefix(compositeKey));
        assertEquals(0L, KeyValueStoreCoalescingZonedScheduler.extractPrefix(null));
        assertEquals(0L, KeyValueStoreCoalescingZonedScheduler.extractPrefix(new byte[] { 1, 2, 3 }));
        assertArrayEquals(new byte[0], KeyValueStoreCoalescingZonedScheduler.extractKeyBytes(null));
        assertArrayEquals(new byte[0], KeyValueStoreCoalescingZonedScheduler.extractKeyBytes(new byte[] { 1, 2, 3 }));

        byte[] reverseLookup = KeyValueStoreCoalescingZonedScheduler.reverseLookupKey(logicalKey);
        byte[] rangeStart = KeyValueStoreCoalescingZonedScheduler.rangeStartKey(42L);

        assertEquals(KeyValueStoreCoalescingZonedScheduler.REVERSE_LOOKUP_PREFIX,
                KeyValueStoreCoalescingZonedScheduler.extractPrefix(reverseLookup));
        assertArrayEquals(logicalKey, KeyValueStoreCoalescingZonedScheduler.extractKeyBytes(reverseLookup));
        assertEquals(42L, KeyValueStoreCoalescingZonedScheduler.extractPrefix(rangeStart));
        assertArrayEquals(new byte[0], KeyValueStoreCoalescingZonedScheduler.extractKeyBytes(rangeStart));
    }

    private long lookupPosition(String key) {
        byte[] raw = store.rawGet(KeyValueStoreCoalescingZonedScheduler.reverseLookupKey(keyBytes(key)));
        return KeyValueStoreCoalescingZonedScheduler.decodeLong(raw);
    }

    private List<ScheduledItem<String, String>> invokeDrainRange(byte[] fromInclusive, byte[] toExclusive, int limit)
            throws Exception {
        Method method = KeyValueStoreCoalescingZonedScheduler.class.getDeclaredMethod("drainRange", byte[].class,
                byte[].class,
                int.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<ScheduledItem<String, String>> drained = (List<ScheduledItem<String, String>>) method.invoke(scheduler,
                fromInclusive, toExclusive, limit);
        return drained;
    }

    private static List<String> drainedKeys(List<ScheduledItem<String, String>> items) {
        return items.stream().map(ScheduledItem::key).toList();
    }

    private static List<String> drainedPayloads(List<ScheduledItem<String, String>> items) {
        return items.stream().map(ScheduledItem::payload).toList();
    }

    private static byte[] keyBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static Instant observedAt(long offsetSeconds) {
        return Instant.ofEpochSecond(KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY + offsetSeconds);
    }
}
