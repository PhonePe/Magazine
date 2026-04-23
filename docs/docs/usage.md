# Usage

## Core Operations

Every `Magazine<T>` instance exposes six operations:

| Method | Description |
|--------|-------------|
| `load(T data)` | Enqueue data into the magazine. |
| `fire()` | Dequeue and return the next item. |
| `reload(T data)` | Re-enqueue data (decrements fire counter instead of incrementing load counter). |
| `delete(MagazineData<T>)` | Remove a specific record from the backend. |
| `getMetaData()` | Retrieve per-shard counters and pointers. |
| `peek(Map<Integer, Set<Long>>)` | Read specific shard/pointer records without consuming. |

## Creating a Magazine

=== "Aerospike"

    ```java
    IAerospikeClient client = new AerospikeClient("localhost", 3000);

    AerospikeStorageConfig config = AerospikeStorageConfig.builder()
            .namespace("test")
            .dataSetName("mag_data")
            .metaSetName("mag_meta")
            .shards(64)
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

=== "HBase (planned)"

    ```java
    // HBase backend is not yet implemented.
    // All operations currently throw UnsupportedOperationException.
    HBaseStorage<String> storage = new HBaseStorage<>(
            recordTtl, metaDataTtl, farmId,
            false, 1, clientId, MagazineScope.LOCAL);

    Magazine<String> magazine = Magazine.<String>builder()
            .baseMagazineStorage(storage)
            .magazineIdentifier("orders")
            .build();
    ```

## Loading Data

Enqueue data into the magazine. Each call atomically increments the load pointer and load counter for the selected shard.

```java
boolean success = magazine.load("order-12345");
```

When de-duplication is enabled, a distributed lock is acquired before writing. If the data already exists, the call returns `true` without writing a duplicate.

!!! info "De-duplication support"
    De-duplication is supported only for `String`, `Long`, and `Integer` data types.

## Firing (Consuming) Data

Dequeue the next item. The library randomly selects an active shard and retries until a non-null record is found or all pointers are exhausted.

```java
MagazineData<String> fired = magazine.fire();

String value  = fired.getData();            // the payload
long pointer  = fired.getFirePointer();     // position in shard
Integer shard = fired.getShard();           // shard index
String magId  = fired.getMagazineIdentifier();
```

!!! info "Fire retry behaviour"
    If there are no active shards (i.e. nothing to fire), `fire()` throws a `MagazineException` with `NOTHING_TO_FIRE` immediately — it does **not** retry. The internal infinite retry (`neverStop`) only kicks in when active shards exist but the selected record is null (e.g. expired). In that case it keeps selecting another shard until a non-null record is found.

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
    `refresh()` replaces the internal magazine map. Call it whenever your magazine topology changes (e.g. on config reload).

## De-duplication

When `enableDeDupe` is set to `true` on the storage, each `load()` call:

1. Acquires a distributed lock keyed on `{magazineIdentifier}_{data.toString()}`.
2. Checks whether the data already exists in a deduper set.
3. Writes the data only if it doesn't already exist.
4. Stores the data hash for future de-dup checks.

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

| Scope | Lock Level | Set Name Resolution | Status |
|-------|------------|---------------------|--------|
| `LOCAL` | `DC` (data-centre) | `{farmId}_{setName}` | ✅ Supported |
| `GLOBAL` | `XDC` (cross-DC) | `{setName}` (no prefix) | ❌ Not yet implemented |

```java
// Local scope (recommended)
.scope(MagazineScope.LOCAL)

// Global scope (throws NOT_IMPLEMENTED at runtime)
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

