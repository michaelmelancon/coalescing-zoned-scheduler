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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsKeyValueStoreTest {

    private InMemoryByteArrayKeyValueStore kafkaStore;
    private KafkaStreamsKeyValueStore store;

    @BeforeEach
    void setUp() {
        kafkaStore = new InMemoryByteArrayKeyValueStore();
        store = new KafkaStreamsKeyValueStore(kafkaStore);
    }

    @Test
    void getReturnsNullForMissingKey() {
        assertNull(store.get(new byte[] { 1 }));
    }

    @Test
    void putAndGetStoreValue() {
        store.put(new byte[] { 1 }, new byte[] { 42 });
        assertArrayEquals(new byte[] { 42 }, store.get(new byte[] { 1 }));
    }

    @Test
    void deleteRemovesKey() {
        store.put(new byte[] { 1 }, new byte[] { 42 });
        store.delete(new byte[] { 1 });
        assertNull(store.get(new byte[] { 1 }));
    }

    @Test
    void rangeIteratesOrderedEntries() {
        store.put(new byte[] { 1 }, new byte[] { 10 });
        store.put(new byte[] { 2 }, new byte[] { 20 });

        try (var iter = store.range(new byte[] { 1 }, new byte[] { 3 })) {
            assertTrue(iter.hasNext());
            var e1 = iter.next();
            assertArrayEquals(new byte[] { 1 }, e1.key());
            assertArrayEquals(new byte[] { 10 }, e1.value());
            assertTrue(iter.hasNext());
            var e2 = iter.next();
            assertArrayEquals(new byte[] { 2 }, e2.key());
            assertFalse(iter.hasNext());
        }
    }

    @Test
    void rangeIteratorCloses() {
        try (var iter = store.range(new byte[] { 1 }, new byte[] { 2 })) {
            assertFalse(iter.hasNext());
        }
    }
}
