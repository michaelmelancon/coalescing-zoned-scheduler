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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

class InMemoryKeyValueStoreTest {

    @Test
    void getReturnsNullForMissingKey() {
        var store = new InMemoryKeyValueStore();
        assertNull(store.get(new byte[] { 1 }));
    }

    @Test
    void rangeWithNullBoundsIteratesAllEntries() {
        var store = new InMemoryKeyValueStore();
        store.put(new byte[] { 1 }, new byte[] { 10 });
        store.put(new byte[] { 2 }, new byte[] { 20 });

        try (var iter = store.range(null, null)) {
            assertTrue(iter.hasNext());
            assertArrayEquals(new byte[] { 1 }, iter.next().key());
            assertTrue(iter.hasNext());
            assertArrayEquals(new byte[] { 2 }, iter.next().key());
            assertFalse(iter.hasNext());
        }
    }

    @Test
    void rangeIteratorNextThrowsWhenExhausted() {
        var store = new InMemoryKeyValueStore();
        try (var iter = store.range(null, null)) {
            assertFalse(iter.hasNext());
            assertThrows(NoSuchElementException.class, iter::next);
        }
    }
}
