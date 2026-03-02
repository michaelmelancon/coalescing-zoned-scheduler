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
    private CoalescingZonedScheduler<String, String> scheduler;

    @BeforeEach
    void setUp() {
        store = new InMemoryByteArrayKeyValueStore();
        scheduler = new CoalescingZonedScheduler<>(store, Serdes.String(), Serdes.String());
    }

    @Test
    void scheduleNowAddsImmediateEntryAndTracksCounts() {
        scheduler.scheduleNow("alpha", "payload-1");

        assertTrue(scheduler.isScheduled("alpha"));
        assertEquals(1, scheduler.sizeNow());
        assertEquals(0, scheduler.sizeObserved());
        assertEquals(1, scheduler.sizeTotal());
        assertEquals(CoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, lookupPosition("alpha"));

        List<ScheduledItem<String, String>> drained = scheduler.drainImmediate(10);

        assertEquals(List.of("alpha"), drainedKeys(drained));
        assertEquals(List.of("payload-1"), drainedPayloads(drained));
        assertFalse(scheduler.isScheduled("alpha"));
        assertEquals(0, scheduler.sizeNow());
        assertEquals(0, scheduler.sizeObserved());
        assertEquals(0, scheduler.sizeTotal());
    }

    @Test
    void scheduleNowDoesNothingWhenKeyIsAlreadyImmediate() {
        scheduler.scheduleNow("alpha", "first");
        scheduler.scheduleNow("alpha", "second");

        assertEquals(1, scheduler.sizeNow());
        assertEquals(CoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, lookupPosition("alpha"));

        List<ScheduledItem<String, String>> drained = scheduler.drainImmediate(10);

        assertEquals(List.of("alpha"), drainedKeys(drained));
        assertEquals(List.of("first"), drainedPayloads(drained));
    }

    @Test
    void scheduleNowPromotesObservedEntriesToImmediate() {
        long delayedPosition = CoalescingZonedScheduler.OBSERVED_BOUNDARY + 10;
        scheduler.scheduleAtEpochSeconds("alpha", "delayed", delayedPosition);

        scheduler.scheduleNow("alpha", "promoted");

        assertEquals(1, scheduler.sizeNow());
        assertEquals(0, scheduler.sizeObserved());
        assertFalse(store.containsRawKey(
                CoalescingZonedScheduler.compositeKey(delayedPosition, keyBytes("alpha"))));

        List<ScheduledItem<String, String>> drained = scheduler.drainReady(10, observedAt(20));

        assertEquals(List.of("alpha"), drainedKeys(drained));
        assertEquals(List.of("promoted"), drainedPayloads(drained));
    }

    @Test
    void scheduleAtAddsObservedEntriesAndRewritesObservedSchedules() {
        scheduler.scheduleAtEpochSeconds("alpha", "first", CoalescingZonedScheduler.OBSERVED_BOUNDARY + 10);
        scheduler.scheduleAt("alpha", "second", observedAt(20));

        assertEquals(0, scheduler.sizeNow());
        assertEquals(1, scheduler.sizeObserved());
        assertEquals(1, scheduler.sizeTotal());
        assertTrue(scheduler.drainReady(10, observedAt(15)).isEmpty());

        List<ScheduledItem<String, String>> drained = scheduler.drainReady(10, observedAt(21));

        assertEquals(List.of("alpha"), drainedKeys(drained));
        assertEquals(List.of("second"), drainedPayloads(drained));
        assertEquals(0, scheduler.sizeObserved());
    }

    @Test
    void scheduleAtDoesNothingWhenKeyIsAlreadyImmediate() {
        scheduler.scheduleNow("alpha", "now");

        scheduler.scheduleAt("alpha", "later", observedAt(10));

        assertEquals(1, scheduler.sizeNow());
        assertEquals(0, scheduler.sizeObserved());
        assertEquals(List.of("now"), drainedPayloads(scheduler.drainImmediate(10)));
    }

    @Test
    void scheduleAtEpochSecondsRejectsPositionsBeforeObservedBoundary() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> scheduler.scheduleAtEpochSeconds("alpha", "payload",
                        CoalescingZonedScheduler.OBSERVED_BOUNDARY - 1));

        assertTrue(thrown.getMessage().contains("epochSeconds must be >="));
    }

    @Test
    void drainReadyPrioritizesImmediateThenEligibleObservedAndHonorsLimit() {
        scheduler.scheduleAtEpochSeconds("delayed-1", "observed-1", CoalescingZonedScheduler.OBSERVED_BOUNDARY + 10);
        scheduler.scheduleNow("immediate-1", "now-1");
        scheduler.scheduleAtEpochSeconds("delayed-2", "observed-2", CoalescingZonedScheduler.OBSERVED_BOUNDARY + 20);
        scheduler.scheduleNow("immediate-2", "now-2");

        List<ScheduledItem<String, String>> firstBatch = scheduler.drainReady(3, observedAt(15));

        assertEquals(List.of("immediate-1", "immediate-2", "delayed-1"), drainedKeys(firstBatch));
        assertEquals(List.of("now-1", "now-2", "observed-1"), drainedPayloads(firstBatch));
        assertEquals(0, scheduler.sizeNow());
        assertEquals(1, scheduler.sizeObserved());

        List<ScheduledItem<String, String>> secondBatch = scheduler.drainReady(10, observedAt(25));

        assertEquals(List.of("delayed-2"), drainedKeys(secondBatch));
        assertEquals(List.of("observed-2"), drainedPayloads(secondBatch));
        assertEquals(0, scheduler.sizeTotal());
    }

    @Test
    void drainReadyWithZeroLimitLeavesQueuedEntriesUntouched() {
        scheduler.scheduleNow("alpha", "now");

        List<ScheduledItem<String, String>> drained = scheduler.drainReady(0, observedAt(10));

        assertTrue(drained.isEmpty());
        assertTrue(scheduler.isScheduled("alpha"));
        assertEquals(1, scheduler.sizeNow());
        assertEquals(0, scheduler.sizeObserved());
    }

    @Test
    void cancelRemovesEntriesAndResetsImmediateCounterWhenEmpty() {
        assertFalse(scheduler.cancel("missing"));

        scheduler.scheduleNow("alpha", "now");
        assertTrue(scheduler.cancel("alpha"));
        assertEquals(0, scheduler.sizeNow());

        scheduler.scheduleNow("beta", "next");
        assertEquals(CoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, lookupPosition("beta"));

        scheduler.scheduleAt("gamma", "later", observedAt(30));
        assertTrue(scheduler.cancel("gamma"));
        assertFalse(scheduler.cancel("gamma"));
        assertEquals(1, scheduler.sizeNow());
        assertEquals(0, scheduler.sizeObserved());
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
        CoalescingZonedScheduler<String, String> nullableScheduler =
                new CoalescingZonedScheduler<>(store, nullableKeySerde, Serdes.String());

        byte[] malformedKey = CoalescingZonedScheduler.rangeStartKey(CoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION);
        byte[] nullKey = CoalescingZonedScheduler.compositeKey(
                CoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION + 1,
                keyBytes("null-key"));
        byte[] validKey = CoalescingZonedScheduler.compositeKey(
                CoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION + 2,
                keyBytes("valid"));

        store.put(malformedKey, keyBytes("ignored"));
        store.put(nullKey, keyBytes("ignored-null"));
        store.put(validKey, keyBytes("payload"));

        List<ScheduledItem<String, String>> drained = nullableScheduler.drainImmediate(10);

        assertEquals(List.of("valid"), drainedKeys(drained));
        assertEquals(List.of("payload"), drainedPayloads(drained));
        assertTrue(store.containsRawKey(malformedKey));
        assertTrue(store.containsRawKey(nullKey));
        assertFalse(store.containsRawKey(validKey));
    }

    @Test
    void privateDrainRangeSupportsOpenEndedUpperBound() throws Exception {
        scheduler.scheduleNow("immediate", "now");
        scheduler.scheduleAt("observed", "later", observedAt(5));

        List<ScheduledItem<String, String>> drained = invokeDrainRange(
                CoalescingZonedScheduler.rangeStartKey(CoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION),
                null,
                10);

        assertEquals(List.of("immediate", "observed"), drainedKeys(drained));
        assertEquals(List.of("now", "later"), drainedPayloads(drained));
        assertEquals(0, scheduler.sizeTotal());
    }

    @Test
    void staticHelpersEncodeAndExtractValues() {
        byte[] logicalKey = keyBytes("alpha");
        byte[] compositeKey = CoalescingZonedScheduler.compositeKey(42L, logicalKey);

        assertTrue(CoalescingZonedScheduler.isImmediate(CoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION));
        assertTrue(CoalescingZonedScheduler.isImmediate(CoalescingZonedScheduler.OBSERVED_BOUNDARY - 1));
        assertFalse(CoalescingZonedScheduler.isImmediate(CoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION - 1));
        assertFalse(CoalescingZonedScheduler.isImmediate(CoalescingZonedScheduler.OBSERVED_BOUNDARY));

        assertEquals(42L, CoalescingZonedScheduler.decodeLong(CoalescingZonedScheduler.encodeLong(42L)));
        assertEquals(0L, CoalescingZonedScheduler.decodeLong(null));
        assertEquals(0L, CoalescingZonedScheduler.decodeLong(new byte[] {1, 2, 3}));

        assertArrayEquals(logicalKey, CoalescingZonedScheduler.extractKeyBytes(compositeKey));
        assertEquals(42L, CoalescingZonedScheduler.extractPrefix(compositeKey));
        assertEquals(0L, CoalescingZonedScheduler.extractPrefix(null));
        assertEquals(0L, CoalescingZonedScheduler.extractPrefix(new byte[] {1, 2, 3}));
        assertArrayEquals(new byte[0], CoalescingZonedScheduler.extractKeyBytes(null));
        assertArrayEquals(new byte[0], CoalescingZonedScheduler.extractKeyBytes(new byte[] {1, 2, 3}));

        byte[] reverseLookup = CoalescingZonedScheduler.reverseLookupKey(logicalKey);
        byte[] rangeStart = CoalescingZonedScheduler.rangeStartKey(42L);

        assertEquals(CoalescingZonedScheduler.REVERSE_LOOKUP_PREFIX, CoalescingZonedScheduler.extractPrefix(reverseLookup));
        assertArrayEquals(logicalKey, CoalescingZonedScheduler.extractKeyBytes(reverseLookup));
        assertEquals(42L, CoalescingZonedScheduler.extractPrefix(rangeStart));
        assertArrayEquals(new byte[0], CoalescingZonedScheduler.extractKeyBytes(rangeStart));
    }

    private long lookupPosition(String key) {
        byte[] raw = store.rawGet(CoalescingZonedScheduler.reverseLookupKey(keyBytes(key)));
        return CoalescingZonedScheduler.decodeLong(raw);
    }

    private List<ScheduledItem<String, String>> invokeDrainRange(byte[] fromInclusive, byte[] toExclusive, int limit)
            throws Exception {
        Method method = CoalescingZonedScheduler.class.getDeclaredMethod("drainRange", byte[].class, byte[].class,
                int.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<ScheduledItem<String, String>> drained =
                (List<ScheduledItem<String, String>>) method.invoke(scheduler, fromInclusive, toExclusive, limit);
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
        return Instant.ofEpochSecond(CoalescingZonedScheduler.OBSERVED_BOUNDARY + offsetSeconds);
    }
}
