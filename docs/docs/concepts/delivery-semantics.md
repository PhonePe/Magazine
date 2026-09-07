# Delivery Semantics

**Magazine delivers at most once.** A record can be lost. If your workload cannot tolerate that,
read this page before building on it.

## Why

`fire()` claims a record with a single atomic increment of the shard's fire pointer, guarded so it
only fires when the shard has an unconsumed slot:

1. Atomically advance `FIRE_POINTER` and read back the value it produced.
2. Read the data record at that pointer.
3. Advance `FIRE_COUNTER`.

Step 1 is not idempotent, so it is **never retried**. If the client times out after the server has
applied the increment, the pointer has moved and the caller never sees the record. Retrying would
be worse: it would advance the pointer twice and drop a record that was never delivered.

Timeouts are the only loss window. The claim either happens exactly once or not at all; what the
client cannot always learn is *which*.

## What this means for you

**Every consumer that must not lose work needs its own idempotency record, written before it acts
on the result.** Magazine cannot provide this — it does not know what "done" means for your
payload.

```java
MagazineData<String> record = magazine.fire();
if (!ledger.claim(record.getData())) {   // your durable, idempotent claim
    return;
}
process(record.getData());
```

If you cannot do that, `reload()` republishes a payload so a supervising process can put work back:

```java
try {
    process(record.getData());
} catch (TransientFailure e) {
    magazine.reload(record.getData());   // back on the queue, load counter unchanged
}
```

This narrows the window; it does not close it. A process that dies between `fire()` and `reload()`
still loses the record.

## What is *not* a loss

These look alarming and are not:

- **Holes.** A pointer slot whose data write failed. `fire()` skips them. Nothing was ever
  delivered to a caller, and nothing was lost that had been accepted — `load()` returned false or
  threw.
- **`RETRIES_EXHAUSTED`.** Magazine gave up; data may still exist. Back off and retry.
- **Inflated `pending` on the dashboard.** `FIRE_COUNTER` advances after the data read, so a crash
  between the two leaves it short. It under-counts deliveries, which over-reports pending. It
  cannot hide a shard that still holds data, and it self-corrects as the magazine drains.

## Duplicates

Delivery is at-most-once, so `fire()` will not hand the same record to two consumers. But
duplicates can still enter the queue at the *load* side: if `load()` times out after the server
applied the write, the caller does not know it succeeded and may load the same payload again.

Enable deduplication to suppress that:

```java
AerospikeStorage.<String>builder()
    .enableDeDupe(true)
    // ...
```

The marker is claimed with a single `CREATE_ONLY` write — the server admits exactly one creator of
a key. Two important caveats:

- Only `String`, `Long` and `Integer` payloads are supported, because the marker is keyed on
  `toString()`.
- A concurrent duplicate is indistinguishable from a sequential one. Both report a successful,
  suppressed load (`load()` returns `true`). This changed in 2.0.0; earlier releases raised
  `ACTION_DENIED_PARALLEL_ATTEMPT` for the concurrent case.

The marker is written *before* the payload and withdrawn if the load does not complete, so a failed
write does not suppress a legitimate retry.

## Ordering

Magazine is **not** a FIFO queue.

Within one shard, records are fired in load order. Across shards there is no ordering at all:
`load()` picks a shard at random and `fire()` picks a random *active* shard. With the default of 8
shards, consecutive loads almost certainly land on different shards and can be delivered in any
order.

Set `shards` to `1` if you need global FIFO — at the cost of a single hot metadata record, and
knowing an unsharded magazine can never later be promoted to sharded while it holds data.

## Summary

| Property | Guarantee |
|---|---|
| Delivery | At most once |
| Duplicate delivery | Never — a pointer is claimed by exactly one consumer |
| Duplicate *loads* | Possible; use `enableDeDupe` |
| Ordering within a shard | Load order |
| Ordering across shards | None |
| Loss window | Client timeout on the fire-pointer claim |
