# Upgrading to 2.0

Magazine 2.0 is a deliberate breaking release. This page lists everything you have to change.

## 1. Artifact coordinates

```xml
<!-- 1.x -->
<dependency>
  <groupId>com.phonepe</groupId>
  <artifactId>magazine</artifactId>
</dependency>

<!-- 2.0 -->
<dependency>
  <groupId>com.phonepe</groupId>
  <artifactId>magazine-core</artifactId>
</dependency>
```

`com.phonepe:magazine` is now the aggregator POM, not the library.

## 2. Moved packages

The `common`, `scope` and `util` packages are gone, and `StorageType` left `core`.
Update imports:

| 1.x | 2.0 |
|---|---|
| `com.phonepe.magazine.scope.MagazineScope` | `com.phonepe.magazine.entity.MagazineScope` |
| `com.phonepe.magazine.common.MagazineData` | `com.phonepe.magazine.entity.MagazineData` |
| `com.phonepe.magazine.common.MetaData` | `com.phonepe.magazine.entity.MetaData` |
| `com.phonepe.magazine.core.StorageType` | `com.phonepe.magazine.entity.StorageType` |
| `com.phonepe.magazine.common.Constants` | `com.phonepe.magazine.impl.aerospike.common.AerospikeConstants` |
| `com.phonepe.magazine.util.ErrorMessage` | `com.phonepe.magazine.impl.aerospike.common.ErrorMessage` |

`AerospikeConstants` and `ErrorMessage` describe the Aerospike layout. Treat them as internal.

## 3. Removed types and members

| Removed | Replacement |
|---|---|
| `StorageTypeVisitor` | None. Dispatch goes through `BaseMagazineStorage` polymorphism; `StorageType` is a plain value enum. |
| `MagazineData.createAerospikeKey()` | None. Key construction moved into the backend, which needs the `MagazineContext`. |
| `ErrorCode.ACTION_DENIED_PARALLEL_ATTEMPT` | None. Concurrent duplicate loads now return `true` like any other suppressed duplicate. |
| `AerospikeStorageConfig.maxFireContentionAttempts` | None. Contention was eliminated, not tuned. |
| `CommonUtils.resolveLockLevel()` | None. It returned a DLM `LockLevel`, and the DLM is gone. |
| `CommonUtils.validateMagazineScope()` | None. `BaseMagazineStorage` validates scope at construction. |
| `CommonUtils` set-name resolution | `AerospikeNaming.resolveSetName(...)`, which is internal. The storage resolves set names itself. |
| `AerospikeRetryerFactory` | None. Retrying is internal to the Aerospike backend. |

`MagazineScope.Visitor` is unchanged and still supported.

## 4. Storage SPI changed

Only relevant if you implement `BaseMagazineStorage` yourself.

- `initialize(String)` now returns a **`MagazineContext`** instead of `void`. It is called once per
  `Magazine` and must resolve the persisted configuration.
- Every other SPI method takes a `MagazineContext` as its first parameter.
- The constructor **no longer takes `shards`**. Shard count is a per-magazine property carried on
  the context, not a property of the storage.

```java
// 2.0
protected BaseMagazineStorage(StorageType type, int recordTtl, int metaDataTtl,
                              String farmId, boolean enableDeDupe, String clientId,
                              MagazineScope scope)
```

## 5. Behaviour changes

### Shard count is no longer rewritten at boot

Previously, opening a magazine with a different `shards` value could rewrite persisted state. Now
the **persisted count is authoritative** and your config is only a creation default. Set
`allowShardIncrease=true` to widen an existing magazine.

The upside: one storage instance can now serve magazines with differing shard counts, which used
to fail application startup.

`shards` default dropped from **64 to 8**. Existing magazines are unaffected — their persisted
count wins. New magazines get 8 unless you say otherwise.

### `fire()` is documented at-most-once

The guarantee has not changed, but it was never written down. Read
[delivery semantics](concepts/delivery-semantics.md) before relying on it.

### `RETRIES_EXHAUSTED` and `NOTHING_TO_FIRE` are distinct

A consumer loop that treats only `NOTHING_TO_FIRE` as "queue empty" is correct. One that treats
`RETRIES_EXHAUSTED` the same way will silently stop consuming a queue that still has data.

### Deduplication no longer uses a distributed lock

The DLM dependency was removed outright, along with its transitive graph
(`hbase-shaded-client`, `log4j` 1.2.17, `junit` 4.12 at compile scope, and a Jackson downgrade). If
you were relying on those coming in transitively, declare them yourself.

A concurrent duplicate load now returns `true` rather than throwing.

### Guava is gone

`guava-retrying` was replaced with an in-house retry loop, so Guava is no longer on the compile
classpath. Declare it directly if you depended on it transitively.

## 6. Dropwizard bundle

- **Wire format**: counters and pointers are now plain JSON numbers. They used to be strings.
- `PeekedData` gained a `type` field carrying the payload's simple class name.
- **Totals no longer sum pointers.** Summing per-shard monotonic sequences is meaningless; totals
  report pointers as `0` and per-shard values are shown individually.
- **Peek is now access-controlled** and fails closed. Register authentication that populates a
  `SecurityContext` and grants the `magazine_peek` role. See
  [the bundle guide](dropwizard-bundle.md).

## 7. New, optional

- **Metrics** — Micrometer meters, published to the global registry by default (the Dropwizard
  bundle attaches a backend for you, so there is nothing to wire). See
  [metrics](concepts/metrics.md).
- `activeShardRefreshSeconds` — tune shard-discovery read load.
- `metadataCacheSeconds` on the bundle — throttle dashboard fan-out.

## Checklist

1. Change the artifact to `magazine-core`.
2. Fix `scope`, `common` and `util` imports, and `core.StorageType`.
3. Remove references to `StorageTypeVisitor`, `CommonUtils`, `AerospikeRetryerFactory`,
   `createAerospikeKey()`, `ACTION_DENIED_PARALLEL_ATTEMPT` and `maxFireContentionAttempts`.
4. Handle `RETRIES_EXHAUSTED` separately from `NOTHING_TO_FIRE`.
5. Register auth for peek, or accept it returning 403.
6. Update anything parsing the dashboard JSON for numbers-as-strings.
7. Declare Guava or DLM directly if you were getting them transitively.
