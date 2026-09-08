# Defaults

## `AerospikeStorageConfig`

| Field | Default | Notes |
|---|---|---|
| `namespace` | — | Required. |
| `dataSetName` | — | Required. Prefixed with the farm ID for `LOCAL` scope. |
| `metaSetName` | — | Required. Prefixed with the farm ID for `LOCAL` scope. |
| `recordTtl` | `2592000` (30 days) | Must be positive. |
| `metaDataTtl` | `5184000` (60 days) | Must be **greater than** `recordTtl`. |
| `shards` | `8` | Creation default only — see below. |
| `allowShardIncrease` | `false` | Must be set to widen an existing magazine. |
| `activeShardRefreshSeconds` | `5` | Primary lever on steady-state read load. |
| `maxFireHoleSkips` | `512` | Bounds `fire()`; exhausting it raises `RETRIES_EXHAUSTED`. |
| `metricsEnabled` | `true` | Publishes to Micrometer's global registry unless a `meterRegistry` is set on the builder. |

### `metaDataTtl` must outlive `recordTtl`

Enforced at construction. `fire()` reads the metadata record to locate the next data record, so
metadata expiring first would make live data unreachable and indistinguishable from an
uninitialised magazine.

### `shards` is a creation default, not a setting

The **persisted** shard count is authoritative. When a magazine already exists, its stored count is
adopted and `shards` is ignored — which is what lets one storage instance serve magazines with
differing shard counts.

| Situation | Behaviour |
|---|---|
| `shards` == persisted | Used as-is. |
| `shards` < persisted | Persisted count adopted, logged at INFO. **Not an error** — shards cannot be narrowed. |
| `shards` > persisted, `allowShardIncrease` false | Persisted count adopted, logged at INFO. |
| `shards` > persisted, `allowShardIncrease` true | Widened and persisted via a guarded update. |
| Unsharded (≤1) → sharded, magazine drained | Promoted, logged at WARN. Records under the flat key layout are abandoned and expire with their TTL. |
| Unsharded (≤1) → sharded, records undelivered | `INVALID_SHARDS`. Flat-layout keys carry no `SHARD_<n>` fragment, so those records would become unreachable. |

Boot never rewrites persisted shard state unless you explicitly opt in.

### Choosing `shards` and `activeShardRefreshSeconds`

Shards spread records across Aerospike partitions and stop any single metadata record becoming a
hot key. They do **not** reduce fire-pointer contention — the claim is a guarded atomic increment
and cannot be lost — so a large shard count buys less than it used to.

Every shard widens the active-shard batch read:

```
discovery load ≈ magazines × shards ÷ activeShardRefreshSeconds   key reads/second
```

That cost is paid only while a magazine is being consumed; idle magazines refresh nothing. The
default of 8 shards at 5 seconds is ~1.6 key reads/sec per active magazine. Raise `shards` if a
metadata record becomes hot; raise `activeShardRefreshSeconds` to cut discovery load.

## Internal constants

Not user-configurable. Defined in `com.phonepe.magazine.impl.aerospike.common.AerospikeConstants`.

| Constant | Value | Purpose |
|---|---|---|
| `MAX_RETRIES` | `5` | Attempts per storage call for retryable failures. |
| `AEROSPIKE_RETRY_DELAY_MS` | `10` | Fixed delay between those attempts. |
| `MAX_FIRE_HOLE_SKIPS` | `512` | Default for `maxFireHoleSkips`. |
| `DEFAULT_SHARDS` | `8` | Default for `shards`. |
| `DEFAULT_REFRESH` | `5` | Default for `activeShardRefreshSeconds`. |
| `DEFAULT_MAX_ELEMENTS` | `1024` | Active-shard cache capacity, in magazines. |
| `SHARD_CONFIGURATION_TTL_SECONDS` | `157680000` (5 years) | TTL of the per-magazine shard configuration record. |
| `LEGACY_METADATA_SCHEMA_VERSION` | `0` | Pointers and counters in separate records. |
| `UNIFIED_METADATA_SCHEMA_VERSION` | `2` | Pointers and counters in one record. Used for all new magazines. |

## Retry behaviour

| Path | Attempts | Delay |
|---|---|---|
| Storage reads and writes | 5 | 10 ms fixed |
| Fire-pointer claim | **1** | — |
| `fire()` hole skips | up to `maxFireHoleSkips` | none |

The fire-pointer claim is deliberately never retried: it is a non-idempotent `add`, so a retry
after a timeout could double-advance the pointer and drop a record. See
[delivery semantics](delivery-semantics.md).

There is **no contention retry budget**. Concurrent consumers receive distinct pointers, so a
claim cannot be lost to another consumer and there is nothing to back off from.
