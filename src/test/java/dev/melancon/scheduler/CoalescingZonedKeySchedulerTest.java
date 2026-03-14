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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CoalescingZonedKeySchedulerTest {

    private CoalescingZonedKeyScheduler<String> scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new CoalescingZonedKeyScheduler<>(
                new KeyValueStoreCoalescingZonedScheduler<>(new InMemorySchedulerStore<>()));
    }

    @Test
    void keyOnlySchedulerTracksAndDrainsImmediateAndObservedKeys() {
        scheduler.scheduleNow("immediate");
        scheduler.scheduleLater("delayed", observedAt(10));

        assertTrue(scheduler.isScheduled("immediate"));
        assertTrue(scheduler.isScheduled("delayed"));
        assertEquals(1, scheduler.sizeImmediate());
        assertEquals(1, scheduler.sizeDelayed());
        assertEquals(2, scheduler.sizeTotal());
        assertEquals(List.of("immediate"), scheduler.drain(10, observedAt(0)));
        assertFalse(scheduler.isScheduled("immediate"));
        assertEquals(0, scheduler.sizeImmediate());
        assertEquals(1, scheduler.sizeDelayed());

        assertEquals(List.of("delayed"), scheduler.drain(10));
        assertEquals(0, scheduler.sizeTotal());
    }

    @Test
    void keyOnlySchedulerCancelsScheduledKeys() {
        scheduler.scheduleLater("delayed", observedAt(5));

        assertTrue(scheduler.cancel("delayed"));
        assertFalse(scheduler.cancel("delayed"));
        assertFalse(scheduler.cancel("missing"));
        assertFalse(scheduler.isScheduled("delayed"));
        assertEquals(0, scheduler.sizeImmediate());
        assertEquals(0, scheduler.sizeDelayed());
        assertEquals(0, scheduler.sizeTotal());
    }

    private static Instant observedAt(long offsetSeconds) {
        return Instant.ofEpochSecond(KeyValueStoreCoalescingZonedScheduler.DELAYED_BOUNDARY + offsetSeconds);
    }
}
