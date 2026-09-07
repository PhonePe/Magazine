# Usage

## Core Operations

Every `Magazine<T>` instance exposes these operations:

| Method | Description |
|--------|-------------|
| `load(T data)` | Enqueue data into the magazine. |
| `fire()` | Dequeue and return the next item. |
| `reload(T data)` | Re-enqueue data (decrements fire counter instead of incrementing load counter). |
| `delete(MagazineData<T>)` | Remove a specific record from the backend. |
| `getMetaData()` | Retrieve per-shard counters and pointers. |
| `getShards()` | This magazine's persisted shard count. |
| `peek(Map<Integer, Set<Long>>)` | Read specific shard/pointer records without consuming. |
| `getMagazineIdentifier()` | The identifier this magazine is bound to. |

## Creating a Magazine

```java
IAerospikeClient client = new AerospikeClient("localhost", 3000);

AerospikeStorageConfig config = AerospikeStorageConfig.builder()
        .namespace("test")
        .dataSetName("mag_data")
        .metaSetName("mag_meta")
        .shards(8)
        .recordTtl(30 * 24 * 60 * 60)
        .metaDataTtl(2 * 30 * 24 * 60 * 60)
        .build();

AerospikeStorage<String> storage = AerospikeStorage.<String>builder()
        .aerospikeClient(client)
        .storageConfig(config)
        .enableDeDupe(true)
        .farmId("dc1")
        .clazz(String.class)
        .clientId("my-service")
        .scope(MagazineScope.LOCAL)
        .build();

Magazine<String> magazine = Magazine.<String>builder()
        .baseMagazineStorage(storage)
        .magazineIdentifier("orders")
        .build();
```

## Loading Data

Enqueue data into the magazine. Each successful call advances the load pointer and, after storing the data, increments the load counter for the selected shard.

```java
boolean success = magazine.load("order-12345");
```

When de-duplication is enabled, the write is issued as a create-only operation. If the data already exists, the call returns `true` without writing a duplicate.

!!! info "De-duplication support"
    De-duplication is supported only for `String`, `Long`, and `Integer` data types.

## Firing (Consuming) Data

Dequeue the next item. The library picks an active shard and claims a slot with a guarded atomic increment of that shard's fire pointer.

!!! danger "`fire()` is at-most-once"
    The claim is applied server-side before the payload reaches you. If the client times out after the server applied the claim, the pointer has already advanced and that record will **never** be delivered again. Callers that cannot tolerate loss must persist their own idempotency record before acting on the result. See [Delivery Semantics](concepts/delivery-semantics.md).

```java
MagazineData<String> fired = magazine.fire();

String value  = fired.getData();            // the payload
long pointer  = fired.getFirePointer();     // position in shard
Integer shard = fired.getShard();           // shard index
String magId  = fired.getMagazineIdentifier();
```

!!! info "Two distinct failure modes"
    `fire()` throws a `MagazineException` with one of two error codes, and **treating them alike is a bug**:

    - `NOTHING_TO_FIRE` — the magazine is drained. There is nothing to consume; back off and retry later.
    - `RETRIES_EXHAUSTED` — the library gave up while skipping consecutive pointer holes (slots whose pointer was allocated but whose data write failed), bounded by `maxFireHoleSkips`. This is *not* an empty queue: data may still exist. Alerting on it, rather than silently treating it as "empty", is the correct response.

    The scan is bounded — there is no retry loop over contention, because the claim is a guarded atomic increment and can never be lost. Exhausted shards are suppressed in the process-local active-shard cache, so loads from another instance can take up to `activeShardRefreshSeconds` (default 5) to become visible.

## Reloading Data

Re-enqueue data that was previously fired. This increments the load pointer but **decrements the fire counter** instead of incrementing the load counter — preserving accurate total-load counts.

```java
boolean reloaded = magazine.reload("order-12345");
```

## Deleting Data

Remove a specific record from the backend. Requires the `MagazineData` object returned by `fire()` or `peek()`.

```java
MagazineData<String> fired = magazine.fire();
// Process the data...
magazine.delete(fired);
```

!!! warning
    `delete()` throws a `MagazineException` with `INVALID_CONFIGURATION` if the supplied `MagazineData` belongs to a different magazine than the one you call it on. Do not route records returned by one magazine into another's `delete()`.

## Peeking Data

Read records from specific shards and pointers **without modifying any counters or pointers**. Useful for inspection, debugging, or replay scenarios.

```java
Map<Integer, Set<Long>> shardPointers = Map.of(
        0, Set.of(1L, 2L, 3L),
        1, Set.of(1L)
);

Set<MagazineData<String>> results = magazine.peek(shardPointers);
results.forEach(md ->
    System.out.printf("Shard %d, Pointer %d → %s%n",
        md.getShard(), md.getFirePointer(), md.getData()));
```

## Getting Metadata

Retrieve per-shard counters and pointers to understand magazine state.

```java
Map<String, MetaData> meta = magazine.getMetaData();

meta.forEach((shard, m) -> {
    System.out.printf("%-20s load=%d/%d  fire=%d/%d%n",
            shard,
            m.getLoadCounter(), m.getLoadPointer(),
            m.getFireCounter(), m.getFirePointer());
});
```

**Example output:**

```
SHARD_0              load=5/5  fire=2/2
SHARD_1              load=3/3  fire=1/1
SHARD_2              load=0/0  fire=0/0
...
```

## Managing Multiple Magazines

`MagazineManager` acts as a registry for multiple `Magazine` instances, potentially of different types.

```java
MagazineManager manager = new MagazineManager("my-service");

Magazine<String> ordersMag = Magazine.<String>builder()
        .baseMagazineStorage(stringStorage)
        .magazineIdentifier("orders")
        .build();

Magazine<Long> idsMag = Magazine.<Long>builder()
        .baseMagazineStorage(longStorage)
        .magazineIdentifier("id-pool")
        .build();

// Register all magazines
manager.refresh(List.of(ordersMag, idsMag));

// Retrieve by identifier
Magazine<String> orders = manager.getMagazine("orders");
orders.load("new-order-456");

Magazine<Long> ids = manager.getMagazine("id-pool");
MagazineData<Long> id = ids.fire();
```

!!! note
    `refresh()` atomically replaces the internal magazine map. Call it whenever your magazine topology changes (e.g. on config reload).

## De-duplication

When `enableDeDupe` is set to `true` on the storage, each `load()` call writes the de-dup marker with a create-only write, so the server — not a client-side lock — resolves concurrent attempts. The data is written only if no marker already exists.

```java
AerospikeStorage<String> storage = AerospikeStorage.<String>builder()
        .aerospikeClient(client)
        .storageConfig(config)
        .enableDeDupe(true)    // ← enable de-duplication
        .farmId("dc1")
        .clazz(String.class)
        .clientId("my-service")
        .scope(MagazineScope.LOCAL)
        .build();

magazine.load("unique-item");
magazine.load("unique-item"); // no-op — already exists
```

## Scoping

| Scope | Reach | Set Name Resolution | Status |
|-------|-------|---------------------|--------|
| `LOCAL` | Single data-centre | `{farmId}_{setName}` | ✅ Supported |
| `GLOBAL` | Cross-DC | `{setName}` (no prefix) | ❌ Not yet implemented |

```java
// Local scope (recommended)
.scope(MagazineScope.LOCAL)

// Global scope — rejected with NOT_IMPLEMENTED while the storage is
// being constructed, so the application fails at boot.
.scope(MagazineScope.GLOBAL)
```

## Complete Lifecycle Example

```java
// ── Setup ──
IAerospikeClient client = new AerospikeClient("localhost", 3000);

AerospikeStorageConfig config = AerospikeStorageConfig.builder()
        .namespace("production")
        .dataSetName("notification_data")
        .metaSetName("notification_meta")
        .shards(32)
        .recordTtl(7 * 24 * 60 * 60)        // 7 days
        .metaDataTtl(30 * 24 * 60 * 60)     // 30 days
        .build();

AerospikeStorage<String> storage = AerospikeStorage.<String>builder()
        .aerospikeClient(client)
        .storageConfig(config)
        .enableDeDupe(true)
        .farmId("dc1")
        .clazz(String.class)
        .clientId("notification-service")
        .scope(MagazineScope.LOCAL)
        .build();

Magazine<String> magazine = Magazine.<String>builder()
        .baseMagazineStorage(storage)
        .magazineIdentifier("push-notifications")
        .build();

// ── Producer ──
magazine.load("user:1001:Welcome to our app!");
magazine.load("user:1002:Your order has shipped.");

// ── Consumer ──
MagazineData<String> notification = magazine.fire();
sendPushNotification(notification.getData());
magazine.delete(notification);  // clean up after processing

// ── Missed delivery? Reload ──
magazine.reload("user:1001:Welcome to our app!");

// ── Monitoring ──
Map<String, MetaData> meta = magazine.getMetaData();
long totalPending = meta.values().stream()
        .mapToLong(m -> m.getLoadPointer() - m.getFirePointer())
        .sum();
System.out.println("Pending notifications: " + totalPending);
```
