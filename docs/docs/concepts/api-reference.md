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
    The constructor validates the storage backend. For Aerospike, shard count cannot decrease and an unsharded magazine cannot become sharded; accepted increases are persisted. New magazines record metadata schema version 2; versionless magazines continue using the legacy metadata layout.

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `load(T data)` | `boolean` | Enqueue data. Returns `true` on success. |
| `fire()` | `MagazineData<T>` | Dequeue and return the next item. |
| `reload(T data)` | `boolean` | Re-enqueue data (decrements fire counter, not increment load counter). |
| `delete(MagazineData<T> magazineData)` | `void` | Delete a specific record from the backend. |
| `getMetaData()` | `Map<String, MetaData>` | Per-shard metadata (counters and pointers). |
| `getShards()` | `int` | Read the configured shard count without exposing storage operations. |
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
| `refresh(List<Magazine<?>> magazines)` | `void` | Atomically replace all registered magazines. |
| `getMagazine(String magazineIdentifier)` | `Magazine<T>` | Retrieve a magazine by identifier. Throws `MagazineException` with `MAGAZINE_NOT_FOUND` if not found. |

---

## `BaseMagazineStorage<T>`

Abstract base class for storage implementations. Aerospike is currently the only supported backend; additional backends require implementing this contract and integrating their validation.

### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `type` | `StorageType` | Backend type. Currently `AEROSPIKE`. |
| `recordTtl` | `int` | TTL in seconds for data records. Must be positive. |
| `metaDataTtl` | `int` | TTL in seconds for metadata records. Must be greater than `recordTtl`. |
| `farmId` | `String` | Data centre / farm identifier. |
| `enableDeDupe` | `boolean` | Whether to enable write de-duplication. |
| `shards` | `int` | Number of shards. Minimum is 1. |
| `clientId` | `String` | Owning service identifier. |
| `scope` | `MagazineScope` | `LOCAL` or `GLOBAL`. |

### Abstract Methods

| Method | Description |
|--------|-------------|
| `load(MagazineContext context, T data)` | Persist data into the context's magazine. |
| `reload(MagazineContext context, T data)` | Re-persist data (fire counter adjustment). |
| `fire(MagazineContext context)` | Consume the next item from the context's magazine. |
| `getMetaData(MagazineContext context)` | Read per-shard metadata. |
| `delete(MagazineContext context, MagazineData<T> magazineData)` | Delete a specific record. |
| `peek(MagazineContext context, Map<Integer, Set<Long>> shardPointersMap)` | Read without consuming. |

`Magazine` passes an immutable `MagazineContext` to storage operations so the resolved metadata schema version does not need to be read on every call.

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

---

## Thread Safety

- `MagazineManager` publishes an immutable map on each `refresh()`, so concurrent readers observe either the previous or new complete registration set.
- `Magazine<T>` delegates all operations to the storage backend. Thread safety depends on the backend implementation.
- `AerospikeStorage<T>` is thread-safe for all operations. Distributed locks ensure safe concurrent writes when de-duplication is enabled.
