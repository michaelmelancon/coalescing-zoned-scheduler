# Coalescing Zoned Scheduler

A persistent keyed work scheduler for stateful systems.

A **Coalescing Zoned Scheduler** maintains **at most one live scheduled entry per logical key**, organizes entries into **ordered zones** such as immediate and delayed, and supports **coalescing reschedules** and **promotion between zones**.

It is designed for workloads where you want to remember:

> **this key needs work**

rather than preserve every enqueue event forever.

---

## Motivation

Many systems need something queue-like, but a plain queue is the wrong semantic.

Examples:

- an entity needs recomputation
- a materialized view needs rebuilding
- a key should be retried after some delay
- repeated scheduling for the same key should collapse into one pending work item
- delayed work should become urgent if new activity arrives

A normal FIFO queue preserves every enqueue.  
That is often **incorrect** for stateful work scheduling.

What you often really want is:

- **one live scheduled entry per key**
- **immediate work drains first**
- **delayed work becomes eligible later**
- **urgent scheduling can promote delayed scheduling**
- **the scheduler survives restarts**
- **the structure works efficiently on top of an ordered key-value store**

That is the problem this abstraction is meant to solve.

---

## What it is

A **Coalescing Zoned Scheduler** is a persistent scheduling abstraction over logical keys.

Typical zones include:

- **Immediate**
  - eligible now
  - drained in FIFO-like order

- **Observed / Delayed**
  - eligible only after a scheduled time
  - drained in time order

The scheduler coalesces by key, so scheduling the same logical key multiple times does **not** create multiple live entries.

Instead, re-scheduling a key may:

- do nothing
- update its delayed schedule
- promote it from delayed to immediate

---

## What it is not

This is **not**:

- a general FIFO queue
- an append-only delay queue
- a command log
- an audit trail
- a structure that preserves multiple outstanding items for the same key

If your requirement is:

> every submitted item must be preserved and processed independently

then this is the wrong abstraction.

Use a real queue.

---

## Backing store matters

This abstraction has favorable logical complexity, but its practical performance depends strongly on the backing data structure.

It is a particularly good fit for **time-ordered delayed scheduling** on an ordered persistent store such as RocksDB.

It is a weaker fit for **very active immediate-zone scheduling** on RocksDB, because each logical update may translate into multiple physical KV-store mutations and associated storage-engine churn.

For immediate-heavy workloads, an **in-memory ordered implementation** is often a better fit, with scheduler state reconstructed from persisted authoritative state during startup when needed.

---

## Core semantics

### Invariants

1. **At most one live entry per key**  
   A logical key may only have one current scheduled position.

2. **Zone-local ordering**  
   Each zone defines its own ordering behavior.

3. **Zone precedence**  
   Immediate work drains before delayed work.

4. **Coalescing reschedule**  
   Re-scheduling the same key does not create duplicates.

5. **Promotion**  
   A delayed key may be promoted to immediate.

6. **Eligibility-based draining**  
   Only entries currently eligible under zone rules may be drained.

---

## Example API

### Key-only form

This is the most natural form when the scheduler only tracks eligibility and the authoritative state lives elsewhere.

```java
interface ZonedScheduler<K> {
    void scheduleNow(K key);
    void scheduleLater(K key, Instant when);

    boolean cancel(K key);
    boolean isScheduled(K key);

    List<K> drainReady(int limit);

    long sizeImmediate();
    long sizeDelayed();
    long sizeTotal();
}
```

### Expected behavior

#### `scheduleNow(key)`

- schedules `key` in the immediate zone
- if `key` is already immediate, this is a no-op
- if `key` is delayed, it is promoted to immediate

#### `scheduleLater(key, when)`

- schedules `key` in the delayed zone
- if `key` is already immediate, immediate wins
- if `key` is already delayed, replacement behavior depends on policy

#### `drainReady(limit)`

- drains up to `limit` currently eligible entries
- immediate entries drain first
- delayed entries drain only when their scheduled time has passed

---

## Payload-carrying form

A payload-carrying variant is also possible:

```java
interface ZonedScheduler<K, P> {
    void scheduleNow(K key, P payload);
    void scheduleLater(K key, Instant when, P payload);

    boolean cancel(K key);
    boolean isScheduled(K key);

    List<ScheduledItem<K, P>> drainReady(int limit);

    long sizeImmediate();
    long sizeDelayed();
    long sizeTotal();
}
```

This is useful when the scheduled entry itself carries deferred work.

But the key rule still holds:

> **there is at most one live scheduled entry per logical key**

So payload-carrying mode is only correct when **coalescing by key is correct** for the workload.

---

## Why the key-only form is often better

In many stateful systems, the scheduler should only answer:

> which keys need work now?

The actual value can then be read from the authoritative state store when processing begins.

That has some nice properties:

- avoids duplicating authoritative state
- avoids stale queued payloads
- keeps the scheduler focused on eligibility
- makes coalescing semantics cleaner

The payload-carrying form is useful, but it should usually be treated as a specialization of the same scheduling model.

---

## Implementation sketch

A practical implementation can be built on top of an **ordered key-value store** such as RocksDB.

One simple layout looks like this:

### Reverse lookup zone

Maps:

- `logical key -> current scheduled position`

This enforces **uniqueness by key** and allows existing entries to be found and moved.

### Immediate zone

Stores entries in **FIFO-like order** for immediate draining.

### Delayed zone

Stores entries ordered by **scheduled time**.

### Metadata zone

Stores supporting metadata such as:

- entry counts
- next immediate position
- other scheduler bookkeeping

### Key insight

> **The ordered keyspace itself acts as the scheduler.**

Sorted iteration and range scans become the mechanism for draining ready work.

---

## Example behavior

Suppose the scheduler contains:

- `A` in delayed at `10:05`
- `B` in immediate
- `C` in delayed at `10:10`

Then:

- `drainReady()` before `10:05` returns `B`
- `scheduleNow(A)` promotes `A` to immediate
- the next drain returns `A` before any remaining delayed work
- re-scheduling `B` while already immediate does not create a duplicate entry

---

## Performance characteristics and workload fit

This abstraction has clean abstract-data-type semantics, but its real performance depends heavily on the storage engine used underneath it.

### Important note on complexity

At the ADT level, most operations look constant time:

- keyed lookup
- schedule now
- schedule later
- cancel
- promote delayed work to immediate

and draining is linear in the number of drained entries.

However, when implemented on top of a persistent ordered key-value store such as RocksDB, these operations are **not free in practice**.

A single logical scheduling operation may require multiple physical operations such as:

- reading the reverse lookup entry
- deleting an old scheduled position
- writing a new scheduled position
- updating the reverse lookup
- updating metadata and counters

On an LSM-based store, these writes and deletes may also contribute to:

- write amplification
- tombstone accumulation
- compaction overhead
- iterator and cache disruption

So while the abstraction is simple, the realized cost depends strongly on the workload shape.

### Where this abstraction works best on RocksDB

A RocksDB-backed implementation is a strong fit when:

- most scheduled entries are in the **delayed zone**
- delayed entries are mostly inserted in time order
- the **immediate zone is used infrequently**
- promotion from delayed to immediate happens, but is not the dominant traffic pattern
- draining ready delayed work is more important than high-frequency reordering

This workload shape benefits from the natural ordering of the delayed zone and avoids excessive churn in the immediate zone.

### Where RocksDB becomes a poor fit

A RocksDB-backed implementation is a weaker fit when:

- the **immediate zone is very active**
- keys are frequently re-scheduled
- delayed entries are frequently promoted
- entries are often canceled and rewritten
- the scheduler behaves like a hot mutable immediate work queue

In those cases, the abstraction may still be semantically correct, but the cost of maintaining keyed movement and coalescing over a persistent LSM store can become too high.

### Recommended implementation strategy

In practice, different workloads may justify different backing implementations:

- **Persistent ordered KV store implementation**
  - best for delayed, time-ordered, coalescing scheduling
  - especially effective when immediate work is relatively sparse

- **In-memory ordered implementation**
  - better for workloads dominated by immediate-zone activity
  - avoids the churn cost of maintaining hot immediate scheduling directly in RocksDB

A useful hybrid approach is:

1. persist the authoritative state needed to reconstruct scheduling
2. keep the hot immediate scheduler in memory
3. rebuild the in-memory scheduler on startup from persisted state

This preserves the abstraction while allowing each workload to use a backing structure that matches its access pattern.

### Practical guidance

Use this abstraction over RocksDB when the system mostly needs:

> “remember that this key should become eligible at or after time T, unless it becomes urgent first”

Be more cautious when the system mostly needs:

> “rapidly insert, cancel, promote, and drain immediate work at high volume”

That second workload often benefits from an in-memory implementation even if scheduler state is reconstructed from persisted data during startup.

---

## Tradeoffs

This abstraction is intentionally opinionated.

### Strengths

- deduplicates work by key
- supports delayed eligibility
- supports urgent promotion
- persists naturally in ordered KV stores
- fits stateful stream processing well
- avoids preserving redundant queue history

### Limitations

- does **not** preserve every enqueue
- does **not** support multiple outstanding work items per key
- fairness is only as strong as zone ordering guarantees
- delayed timestamp granularity matters
- payload replacement policy must be defined clearly
- backing-store behavior matters as much as ADT behavior for hot workloads

---

## Good use cases

This abstraction works well for:

- entity recomputation scheduling
- materialization rebuild triggers
- stale-after / retry-after processing
- stateful stream processing
- deduplicated deferred work by logical key
- systems where urgent activity should preempt delayed activity

---

## Bad use cases

This abstraction is a poor fit for:

- exact command-history preservation
- append-only work logs
- independent processing of every submit event
- multiple concurrent outstanding commands for the same logical key
- workloads that require strict historical replay
- extremely hot immediate-only scheduling on a persistent LSM-backed store

---

## Naming

Why “scheduler” instead of “queue”?

Because the defining contract is not:

> preserve all queued items in submission order

It is:

> remember that a key requires work, maybe later, unless it becomes urgent first

That is scheduling.

---

## Design choices to document explicitly

Any implementation should define:

- what happens when a delayed key is scheduled again with a different time
- whether payload replacement is latest-wins or something else
- timestamp precision for delayed entries
- ordering guarantees within the same timestamp bucket
- whether immediate ordering counters are recycled when the immediate zone becomes empty
- whether counts are authoritative or derivable metadata
- whether the implementation is backed by a persistent ordered store, an in-memory ordered structure, or a hybrid reconstruction model

These are not minor details. They are part of the behavior contract.

---

## Summary

A **Coalescing Zoned Scheduler** is a persistent keyed scheduler with:

- **uniqueness by key**
- **ordered zones**
- **delayed eligibility**
- **promotion to urgency**
- **eligibility-based draining**

It is best used when the system cares about:

> **which keys need work now or later**

rather than:

> **every historical enqueue event**

If that is your problem shape, this abstraction is a good fit.
