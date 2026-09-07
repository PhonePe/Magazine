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
    The constructor calls `BaseMagazineStorage.initialize(magazineIdentifier)` exactly once and stores the resulting `MagazineContext`. For Aerospike, the persisted shard count is authoritative: it can never decrease, an unsharded magazine can never be promoted to sharded, and an increase is applied **only** when `allowShardIncrease` is set to `true` — otherwise a mismatch fails loudly rather than silently rewriting persisted state. New magazines record metadata schema version 2; versionless magazines continue using the legacy metadata layout.

!!! warning "`GLOBAL` scope fails at construction"
    `MagazineScope.GLOBAL` is rejected inside the `BaseMagazineStorage` constructor with `NOT_IMPLEMENTED`. This happens while the storage is being built — not on first use — so an application configured with `GLOBAL` fails at boot.

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `load(T data)` | `boolean` | Enqueue data. Returns `true` on success. |
| `fire()` | `MagazineData<T>` | Dequeue and return the next item. **At-most-once** — see [Delivery Semantics](delivery-semantics.md). |
| `reload(T data)` | `boolean` | Re-enqueue data (decrements fire counter, not increment load counter). |
| `delete(MagazineData<T> magazineData)` | `void` | Delete a specific record from the backend. |
| `getMetaData()` | `Map<String, MetaData>` | Per-shard metadata (counters and pointers). |
| `getShards()` | `int` | This magazine's **persisted** shard count (from the resolved `MagazineContext`), not the shard count configured on the storage. |
| `peek(Map<Integer, Set<Long>> shardPointersMap)` | `Set<MagazineData<T>>` | Read specific records without consuming. |
| `getMagazineIdentifier()` | `String` | The identifier this magazine is bound to. |

!!! warning "`fire()` is at-most-once"
    A record is claimed by a guarded atomic increment of the fire pointer. If the client times out after the server applied the claim, the pointer has advanced and that record will never be delivered. `fire()` throws `NOTHING_TO_FIRE` when the magazine is drained and `RETRIES_EXHAUSTED` when it gave up while skipping pointer holes — the latter does **not** mean the queue is empty. See [Delivery Semantics](delivery-semantics.md).

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

A single storage instance may serve many magazines, so it holds **no** per-magazine state. Per-magazine configuration is resolved once by `initialize(String)` into a `MagazineContext`, which the caller threads through every subsequent operation.

### Constructor Parameters

Declaration order is `(type, recordTtl, metaDataTtl, farmId, enableDeDupe, clientId, scope)`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `type` | `StorageType` | Backend type. Currently `AEROSPIKE`. |
| `recordTtl` | `int` | TTL in seconds for data records. Must be positive. |
| `metaDataTtl` | `int` | TTL in seconds for metadata records. Must be greater than `recordTtl`. |
| `farmId` | `String` | Data centre / farm identifier. Required, non-blank. |
| `enableDeDupe` | `boolean` | Whether to enable write de-duplication. |
| `clientId` | `String` | Owning service identifier. Required, non-blank. |
| `scope` | `MagazineScope` | `LOCAL`, or `GLOBAL` which throws `NOT_IMPLEMENTED` here at construction time. |

There is no `shards` constructor parameter — shard count is a property of the magazine, carried on `MagazineContext`.

### Abstract Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `initialize(String magazineIdentifier)` | `MagazineContext` | Resolve, creating if absent, the persisted configuration for a magazine. Called exactly once per `Magazine` instance at construction. Must reconcile requested configuration with persisted state and fail loudly on incompatible change. |
| `load(MagazineContext context, T data)` | `boolean` | Persist data into the context's magazine. |
| `reload(MagazineContext context, T data)` | `boolean` | Re-persist data (fire counter adjustment). |
| `fire(MagazineContext context)` | `MagazineData<T>` | Consume the next item from the context's magazine. |
| `getMetaData(MagazineContext context)` | `Map<String, MetaData>` | Read per-shard metadata. |
| `delete(MagazineContext context, MagazineData<T> magazineData)` | `void` | Delete a specific record. |
| `peek(MagazineContext context, Map<Integer, Set<Long>> shardPointersMap)` | `Set<MagazineData<T>>` | Read without consuming. |

Every method except `initialize` takes the `MagazineContext` as its first argument.

---

## `MagazineContext`

Immutable, per-magazine resolved configuration produced by `BaseMagazineStorage.initialize(String)` and passed to every subsequent storage operation. Shard count is a property of the *magazine*, not of the storage: one storage instance may serve magazines with differing shard counts.

| Method | Returns | Description |
|--------|---------|-------------|
| `getMagazineIdentifier()` | `String` | The magazine this context resolves. |
| `getStorageSchemaVersion()` | `int` | Persisted metadata schema version, so it need not be re-read per call. |
| `getShards()` | `int` | The persisted shard count for this magazine. |
| `isUnsharded()` | `boolean` | `true` when `shards <= 1`, in which case keys carry no shard fragment. |
| `shardId(Integer shard)` | `String` | The `SHARD_<n>` key fragment for a shard; `null` maps to shard `0`. Throws `INVALID_SHARDS` when out of range. |

`SHARD_<n>` is part of the public contract — these strings are the keys of the map returned by `Magazine#getMetaData()`.

---

## `MagazineData<T>`

Envelope returned by `fire()` and `peek()`. A plain POJO — it carries no key-construction logic.

| Field | Type | Description |
|-------|------|-------------|
| `data` | `T` | The payload. |
| `firePointer` | `long` | The pointer position within the shard. |
| `shard` | `Integer` | The shard index (nullable for unsharded magazines). |
| `magazineIdentifier` | `String` | The magazine this data belongs to. |

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

| Value | Reach | Set Name | Status |
|-------|-------|----------|--------|
| `LOCAL` | Single DC | `{farmId}_{setName}` | ✅ Supported |
| `GLOBAL` | Cross-DC | `{setName}` | ❌ Not implemented — throws `NOT_IMPLEMENTED` from the `BaseMagazineStorage` constructor |

---

## `StorageType`

Descriptive label for a storage backend. A plain enum — dispatch happens through `BaseMagazineStorage` polymorphism, not through this enum.

| Value | Description |
|-------|-------------|
| `AEROSPIKE` | Aerospike backend. |

---

## Thread Safety

- `MagazineManager` publishes an immutable map on each `refresh()`, so concurrent readers observe either the previous or new complete registration set.
- `Magazine<T>` delegates all operations to the storage backend. Thread safety depends on the backend implementation.
- `MagazineContext` is immutable and safe to share.
- `AerospikeStorage<T>` is thread-safe for all operations. There is no distributed lock: the fire claim is a guarded atomic increment that cannot be lost, and de-duplication relies on a `CREATE_ONLY` write that the server resolves.
