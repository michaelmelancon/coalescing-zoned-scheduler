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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

class InMemorySchedulerStoreTest {

    @Test
    void deleteEntryOnMissingPositionIsNoOp() {
        var store = new InMemorySchedulerStore<String, String>();
        store.deleteEntry(42L, "alpha");

        try (var it = store.scanEntries(0L, null)) {
            assertFalse(it.hasNext());
        }
    }

    @Test
    void deleteEntryLeavesOtherKeysAtSamePosition() {
        var store = new InMemorySchedulerStore<String, String>();
        store.putEntry(42L, "alpha", "payload-a");
        store.putEntry(42L, "beta", "payload-b");

        store.deleteEntry(42L, "alpha");

        try (var it = store.scanEntries(42L, null)) {
            assertTrue(it.hasNext());
            assertEquals("beta", it.next().key());
            assertFalse(it.hasNext());
        }
    }

    @Test
    void scanEntryIteratorThrowsNoSuchElementWhenExhausted() {
        var store = new InMemorySchedulerStore<String, String>();

        try (var it = store.scanEntries(0L, null)) {
            assertFalse(it.hasNext());
            assertThrows(NoSuchElementException.class, it::next);
        }
    }
}
