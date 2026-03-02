package com.example.scheduler;

/**
 * A drained scheduled item consisting of its logical key and payload.
 */
public record ScheduledItem<K, P>(K key, P payload) {
}
