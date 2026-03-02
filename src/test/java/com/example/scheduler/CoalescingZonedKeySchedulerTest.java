package com.example.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CoalescingZonedKeySchedulerTest {

    private CoalescingZonedKeyScheduler<String> scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new CoalescingZonedKeyScheduler<>(new InMemoryByteArrayKeyValueStore(), Serdes.String());
    }

    @Test
    void keyOnlySchedulerTracksAndDrainsImmediateAndObservedKeys() {
        scheduler.scheduleNow("immediate");
        scheduler.scheduleAt("observed", observedAt(10));

        assertTrue(scheduler.isScheduled("immediate"));
        assertTrue(scheduler.isScheduled("observed"));
        assertEquals(1, scheduler.sizeNow());
        assertEquals(1, scheduler.sizeObserved());
        assertEquals(2, scheduler.sizeTotal());
        assertEquals(List.of("immediate"), scheduler.drainImmediate(10));
        assertFalse(scheduler.isScheduled("immediate"));
        assertEquals(0, scheduler.sizeNow());
        assertEquals(1, scheduler.sizeObserved());

        assertEquals(List.of("observed"), scheduler.drainReady(10, observedAt(11)));
        assertEquals(0, scheduler.sizeTotal());
    }

    @Test
    void keyOnlySchedulerCancelsScheduledKeys() {
        scheduler.scheduleAt("observed", observedAt(5));

        assertTrue(scheduler.cancel("observed"));
        assertFalse(scheduler.cancel("observed"));
        assertFalse(scheduler.cancel("missing"));
        assertFalse(scheduler.isScheduled("observed"));
        assertEquals(0, scheduler.sizeNow());
        assertEquals(0, scheduler.sizeObserved());
        assertEquals(0, scheduler.sizeTotal());
    }

    private static Instant observedAt(long offsetSeconds) {
        return Instant.ofEpochSecond(CoalescingZonedScheduler.OBSERVED_BOUNDARY + offsetSeconds);
    }
}
