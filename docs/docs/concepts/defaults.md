# Defaults & Configuration

This page lists all configurable values and their defaults.

## `AerospikeStorageConfig`

Configuration object passed to `AerospikeStorage.builder().storageConfig(...)`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `namespace` | `String` | *(required)* | Aerospike namespace. Must already exist on the cluster. |
| `metaSetName` | `String` | *(required)* | Set name for metadata records (pointers, counters, shard info). |
| `dataSetName` | `String` | *(required)* | Set name for data records. |
| `recordTtl` | `int` | `2592000` (30 days) | TTL in seconds for data records. Must be positive. |
| `metaDataTtl` | `int` | `5184000` (60 days) | TTL in seconds for metadata records. Must be greater than `recordTtl`. |
| `shards` | `int` | `64` | Number of shards in the magazine. Minimum: `1`. |

## `AerospikeStorage` Builder

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `aerospikeClient` | `IAerospikeClient` | *(required)* | An already-connected Aerospike client. The library does **not** manage the client lifecycle. |
| `storageConfig` | `AerospikeStorageConfig` | *(required)* | Storage configuration (see above). |
| `enableDeDupe` | `boolean` | `false` | Enable distributed de-duplication on `load()`. |
| `farmId` | `String` | *(required)* | Data centre / farm identifier. Used in set name resolution for `LOCAL` scope. |
| `clazz` | `Class<T>` | *(required)* | The data type class (e.g. `String.class`). Used for casting on read. |
| `clientId` | `String` | *(required)* | Identifier for the owning service. |
| `scope` | `MagazineScope` | *(required)* | `LOCAL` or `GLOBAL`. |

## Internal Constants

These constants are defined in `com.phonepe.magazine.common.Constants` and are **not user-configurable**.

| Constant | Value | Description |
|----------|-------|-------------|
| `MAX_RETRIES` | `5` | Maximum retry attempts for Aerospike operations. |
| `AEROSPIKE_RETRY_DELAY_MS` | `10` ms | Fixed wait between transient Aerospike retry attempts. |
| `MIN_SHARDS` | `1` | Minimum accepted shard count. |
| `SHARD_CONFIGURATION_TTL_SECONDS` | `157680000` (5 years) | Retention for the shard-configuration record. |
| `DEFAULT_REFRESH` | `5` | Default cache refresh interval (seconds) for the active shards cache. |
| `DEFAULT_MAX_ELEMENTS` | `1024` | Maximum elements in the active shards cache. |
| `DLM_CLIENT_ID` | `"magazine"` | Client ID used for the internal `DistributedLockManager`. |

## Retry Configuration

Standard Aerospike reads and writes, including magazine initialization, are wrapped in a `guava-retrying` retryer. The filtered fire claim is deliberately attempted once because retrying an ambiguous write could consume another item:

| Setting | Standard Operations | Fire Operations |
|---------|---------------------|-----------------|
| Retry on | `AerospikeException` | `null` result (pointer hole) |
| Max attempts | 5 | Continues while an active shard has unscanned pointers |
| Wait between attempts | 10 ms (fixed) | None |
| Block strategy | Thread sleep | None |

!!! info "Fire retry behaviour"
    If no deliverable record is found, `fire()` throws `MagazineException` with `NOTHING_TO_FIRE`. Missing pointers are skipped while `loadPointer > firePointer`, because later records may exist in the same shard. Once all cached active shards are exhausted, the call returns immediately.

## Set Name Resolution

When `scope = LOCAL`, set names are prefixed with the `farmId`:

```
{farmId}_{dataSetName}    → e.g. "dc1_magazine_data"
{farmId}_{metaSetName}    → e.g. "dc1_magazine_meta"
```

When `scope = GLOBAL`, set names are used as-is (no prefix).

## Shard Constraints

On magazine construction, the library validates shard configuration:

| Rule | Enforcement |
|------|-------------|
| Cannot decrease shard count | Throws `MagazineException` with `INVALID_SHARDS` |
| Cannot convert unsharded (<= 1) to sharded (> 1) | Throws `MagazineException` with `INVALID_SHARDS` |
| Minimum shard count | Values below `1` throw `MagazineException` with `INVALID_SHARDS` |

The shard-configuration record is retained for five years because it defines the persistent topology of a magazine. Recreating a magazine with an incompatible shard count before this record expires is rejected.
