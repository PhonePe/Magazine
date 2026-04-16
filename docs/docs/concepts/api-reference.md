# API Reference

## `Magazine<T>`

The primary user-facing class. Each instance wraps a `BaseMagazineStorage<T>` and is bound to a specific `magazineIdentifier`.

### Construction

```java
Magazine<T> magazine = Magazine.<T>builder()
        .baseMagazineStorage(storage)
        .magazineIdentifier("my-magazine")
        .build();
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `baseMagazineStorage` | `BaseMagazineStorage<T>` | The storage backend to delegate operations to. |
| `magazineIdentifier` | `String` | Unique identifier for this magazine. |

!!! note "Validation on construction"
    The constructor validates the storage backend. For Aerospike, it checks whether shard metadata exists and verifies shard count constraints:

    - You **cannot decrease** the shard count of an existing magazine.
    - You **cannot convert** an unsharded magazine (shards ≤ 1) to a sharded one (shards > 1).

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `load(T data)` | `boolean` | Enqueue data. Returns `true` on success. |
| `fire()` | `MagazineData<T>` | Dequeue and return the next item. |
| `reload(T data)` | `boolean` | Re-enqueue data (decrements fire counter, not increment load counter). |
| `delete(MagazineData<T> magazineData)` | `void` | Delete a specific record from the backend. |
| `getMetaData()` | `Map<String, MetaData>` | Per-shard metadata (counters and pointers). |
| `peek(Map<Integer, Set<Long>> shardPointersMap)` | `Set<MagazineData<T>>` | Read specific records without consuming. |

---

## `MagazineManager`

A facade for managing multiple `Magazine` instances by their identifier.

### Construction

```java
MagazineManager manager = new MagazineManager("my-client-id");
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `clientId` | `String` | Identifier for the owning service / client. |

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `refresh(List<Magazine<?>> magazines)` | `void` | Register or replace all magazines in the internal map. |
| `getMagazine(String magazineIdentifier)` | `Magazine<T>` | Retrieve a magazine by identifier. Throws `MagazineException` with `MAGAZINE_NOT_FOUND` if not found. |

---

## `BaseMagazineStorage<T>`

Abstract base class for all storage backends. Extend this to implement a custom backend.

### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `type` | `StorageType` | `AEROSPIKE` or `HBASE`. |
| `recordTtl` | `int` | TTL in seconds for data records. |
| `metaDataTtl` | `int` | TTL in seconds for metadata records. |
| `farmId` | `String` | Data centre / farm identifier. |
| `enableDeDupe` | `boolean` | Whether to enable write de-duplication. |
| `shards` | `int` | Number of shards. Minimum is 1. |
| `clientId` | `String` | Owning service identifier. |
| `scope` | `MagazineScope` | `LOCAL` or `GLOBAL`. |

### Abstract Methods

| Method | Description |
|--------|-------------|
| `load(String magazineIdentifier, T data)` | Persist data into the named magazine. |
| `reload(String magazineIdentifier, T data)` | Re-persist data (fire counter adjustment). |
| `fire(String magazineIdentifier)` | Consume next item from the named magazine. |
| `getMetaData(String magazineIdentifier)` | Read per-shard metadata. |
| `delete(MagazineData<T> magazineData)` | Delete a specific record. |
| `peek(String magazineIdentifier, Map<Integer, Set<Long>> shardPointersMap)` | Read without consuming. |

---

## `MagazineData<T>`

Envelope returned by `fire()` and `peek()`.

| Field | Type | Description |
|-------|------|-------------|
| `data` | `T` | The payload. |
| `firePointer` | `long` | The pointer position within the shard. |
| `shard` | `Integer` | The shard index (nullable for unsharded magazines). |
| `magazineIdentifier` | `String` | The magazine this data belongs to. |

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `createAerospikeKey()` | `String` | Constructs the Aerospike record key: `{id}_SHARD_{shard}_{pointer}` or `{id}_{pointer}`. |

---

## `MetaData`

Per-shard state information.

| Field | Type | Description |
|-------|------|-------------|
| `loadCounter` | `long` | Total number of successful `load()` calls for this shard. |
| `fireCounter` | `long` | Total number of successful `fire()` calls for this shard. |
| `loadPointer` | `long` | Current load pointer position. |
| `firePointer` | `long` | Current fire pointer position. |

---

## `MagazineScope`

Enum controlling the scope of magazine operations.

| Value | Lock Level | Set Name | Status |
|-------|------------|----------|--------|
| `LOCAL` | `DC` | `{farmId}_{setName}` | ✅ Supported |
| `GLOBAL` | `XDC` | `{setName}` | ❌ Not implemented (throws `MagazineException`) |

---

## `StorageType`

Enum for supported backend types. Uses the Visitor pattern.

| Value | Description |
|-------|-------------|
| `AEROSPIKE` | Aerospike backend. |
| `HBASE` | HBase backend (planned). |

---

## Thread Safety

- `MagazineManager` uses a `HashMap` internally — it is **not thread-safe** for concurrent `refresh()` and `getMagazine()` calls. Synchronise externally if needed.
- `Magazine<T>` delegates all operations to the storage backend. Thread safety depends on the backend implementation.
- `AerospikeStorage<T>` is thread-safe for all operations. Distributed locks ensure safe concurrent writes when de-duplication is enabled.

