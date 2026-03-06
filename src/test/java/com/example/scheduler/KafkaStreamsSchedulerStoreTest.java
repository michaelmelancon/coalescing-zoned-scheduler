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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsSchedulerStoreTest {

    private InMemoryByteArrayKeyValueStore kafkaStore;
    private KafkaStreamsSchedulerStore<String, String> store;

    @BeforeEach
    void setUp() {
        kafkaStore = new InMemoryByteArrayKeyValueStore();
        store = new KafkaStreamsSchedulerStore<>(kafkaStore, Serdes.String(), Serdes.String());
    }

    // -------------------------------------------------------------------------
    // Position index
    // -------------------------------------------------------------------------

    @Test
    void getPositionReturnsNullForUnknownKey() {
        assertNull(store.getPosition("alpha"));
    }

    @Test
    void putAndGetPosition() {
        store.putPosition("alpha", 42L);
        assertEquals(42L, store.getPosition("alpha"));
    }

    @Test
    void deletePositionRemovesKey() {
        store.putPosition("alpha", 42L);
        store.deletePosition("alpha");
        assertNull(store.getPosition("alpha"));
    }

    // -------------------------------------------------------------------------
    // Scheduled entries
    // -------------------------------------------------------------------------

    @Test
    void putEntryAndScanEntries() {
        store.putEntry(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, "alpha", "payload");

        try (var it = store.scanEntries(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION,
                KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY)) {
            assertTrue(it.hasNext());
            SchedulerStore.Entry<String, String> e = it.next();
            assertEquals(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, e.position());
            assertEquals("alpha", e.key());
            assertEquals("payload", e.payload());
            assertFalse(it.hasNext());
        }
    }

    @Test
    void deleteEntryRemovesEntry() {
        store.putEntry(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, "alpha", "payload");
        store.deleteEntry(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, "alpha");

        try (var it = store.scanEntries(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION,
                KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY)) {
            assertFalse(it.hasNext());
        }
    }

    @Test
    void scanEntriesWithNullUpperBoundReturnsAllEntries() {
        store.putEntry(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, "alpha", "now");
        store.putEntry(KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY + 5, "beta", "later");

        try (var it = store.scanEntries(KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION, null)) {
            assertTrue(it.hasNext());
            assertEquals("alpha", it.next().key());
            assertTrue(it.hasNext());
            assertEquals("beta", it.next().key());
            assertFalse(it.hasNext());
        }
    }

    @Test
    void scanEntriesSkipsMalformedAndNullDeserializedKeys() {
        Serde<String> nullableKeySerde = Serdes.serdeFrom(
                (Serializer<String>) (topic, data) -> (data == null) ? null
                        : data.getBytes(StandardCharsets.UTF_8),
                (Deserializer<String>) (topic, data) -> {
                    if (data == null) {
                        return null;
                    }
                    String v = new String(data, StandardCharsets.UTF_8);
                    return v.equals("null-key") ? null : v;
                });
        KafkaStreamsSchedulerStore<String, String> nullableStore = new KafkaStreamsSchedulerStore<>(kafkaStore,
                nullableKeySerde, Serdes.String());

        long base = KeyValueStoreCoalescingZonedScheduler.FIRST_IMMEDIATE_POSITION;
        byte[] malformedKey = KafkaStreamsSchedulerStore.rangeStartKey(base);
        byte[] nullKeyEntry = KafkaStreamsSchedulerStore.compositeKey(base + 1, "null-key".getBytes(StandardCharsets.UTF_8));
        byte[] validKeyEntry = KafkaStreamsSchedulerStore.compositeKey(base + 2, "valid".getBytes(StandardCharsets.UTF_8));

        kafkaStore.put(malformedKey, "ignored".getBytes(StandardCharsets.UTF_8));
        kafkaStore.put(nullKeyEntry, "ignored-null".getBytes(StandardCharsets.UTF_8));
        kafkaStore.put(validKeyEntry, "payload".getBytes(StandardCharsets.UTF_8));

        List<SchedulerStore.Entry<String, String>> collected = new java.util.ArrayList<>();
        try (var it = nullableStore.scanEntries(base, KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY)) {
            while (it.hasNext()) {
                collected.add(it.next());
            }
        }

        assertEquals(1, collected.size());
        assertEquals("valid", collected.get(0).key());
        assertEquals("payload", collected.get(0).payload());
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Test
    void getMetadataReturnsZeroWhenAbsent() {
        assertEquals(0L, store.getMetadata("missing"));
    }

    @Test
    void putAndGetMetadata() {
        store.putMetadata("counter", 99L);
        assertEquals(99L, store.getMetadata("counter"));
    }

    // -------------------------------------------------------------------------
    // Byte-encoding helpers
    // -------------------------------------------------------------------------

    @Test
    void staticHelpersEncodeAndExtractValues() {
        byte[] logicalKey = "alpha".getBytes(StandardCharsets.UTF_8);
        byte[] compositeKey = KafkaStreamsSchedulerStore.compositeKey(42L, logicalKey);

        assertArrayEquals(logicalKey, KafkaStreamsSchedulerStore.extractKeyBytes(compositeKey));
        assertEquals(42L, KafkaStreamsSchedulerStore.extractPrefix(compositeKey));

        assertEquals(0L, KafkaStreamsSchedulerStore.extractPrefix(null));
        assertEquals(0L, KafkaStreamsSchedulerStore.extractPrefix(new byte[] { 1, 2, 3 }));
        assertArrayEquals(new byte[0], KafkaStreamsSchedulerStore.extractKeyBytes(null));
        assertArrayEquals(new byte[0], KafkaStreamsSchedulerStore.extractKeyBytes(new byte[] { 1, 2, 3 }));

        byte[] rangeStart = KafkaStreamsSchedulerStore.rangeStartKey(42L);
        assertEquals(42L, KafkaStreamsSchedulerStore.extractPrefix(rangeStart));
        assertArrayEquals(new byte[0], KafkaStreamsSchedulerStore.extractKeyBytes(rangeStart));

        assertEquals(42L,
                KafkaStreamsSchedulerStore.decodeLong(KafkaStreamsSchedulerStore.encodeLong(42L)));
        assertEquals(0L, KafkaStreamsSchedulerStore.decodeLong(null));
        assertEquals(0L, KafkaStreamsSchedulerStore.decodeLong(new byte[] { 1, 2, 3 }));

        assertEquals(KafkaStreamsSchedulerStore.POSITION_INDEX_PREFIX,
                KafkaStreamsSchedulerStore.extractPrefix(
                        KafkaStreamsSchedulerStore.compositeKey(KafkaStreamsSchedulerStore.POSITION_INDEX_PREFIX,
                                logicalKey)));
    }

    // -------------------------------------------------------------------------
    // Full scheduler integration via KafkaStreamsSchedulerStore
    // -------------------------------------------------------------------------

    @Test
    void schedulerWorksEndToEndWithKafkaStreamsStore() {
        var scheduler = new KeyValueStoreCoalescingZonedScheduler<>(store);

        scheduler.scheduleNow("alpha", "payload-alpha");
        scheduler.scheduleLater("beta", "payload-beta",
                Instant.ofEpochSecond(KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY + 10));

        assertEquals(1, scheduler.sizeImmediate());
        assertEquals(1, scheduler.sizeDelayed());

        List<ScheduledItem<String, String>> drained = scheduler.drain(10,
                Instant.ofEpochSecond(KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY + 15));

        assertEquals(List.of("alpha", "beta"), drained.stream().map(ScheduledItem::key).toList());
        assertEquals(0, scheduler.sizeTotal());
    }
}
