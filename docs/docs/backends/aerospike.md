# Aerospike Backend

`AerospikeStorage` is the only backend shipped today. It stores payloads and per-shard pointers as
plain Aerospike records and relies on atomic bin operations rather than locks.

## Configuration

```java
AerospikeStorageConfig config = AerospikeStorageConfig.builder()
        .namespace("test")
        .dataSetName("magazine_data")
        .metaSetName("magazine_meta")
        .shards(8)                          // creation default only
        .recordTtl(30 * 24 * 60 * 60)       // 30 days
        .metaDataTtl(2 * 30 * 24 * 60 * 60) // 60 days, must exceed recordTtl
        .build();

AerospikeStorage<String> storage = AerospikeStorage.<String>builder()
        .aerospikeClient(aerospikeClient)
        .storageConfig(config)
        .enableDeDupe(true)
        .farmId("dc1")
        .clazz(String.class)
        .clientId("my-service")
        .scope(MagazineScope.LOCAL)
        .build();
```

### Builder parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `aerospikeClient` | `IAerospikeClient` | *required* | Already connected. Magazine does **not** manage its lifecycle. |
| `storageConfig` | `AerospikeStorageConfig` | *required* | See [defaults](../concepts/defaults.md). |
| `clazz` | `Class<T>` | *required* | Payload type, used to cast on read. |
| `farmId` | `String` | *required* | Data centre identifier; prefixes set names under `LOCAL` scope. |
| `clientId` | `String` | *required* | Owning service; names the deduplication set. |
| `scope` | `MagazineScope` | *required* | `LOCAL`. `GLOBAL` throws `NOT_IMPLEMENTED` at construction. |
| `enableDeDupe` | `boolean` | `false` | Only `String`, `Long`, `Integer` payloads. |
| `meterRegistry` | `MeterRegistry` | Micrometer's global registry | Only needed to isolate a storage from the global registry. See [metrics](../concepts/metrics.md). |

Config fields and their defaults are documented in [Defaults](../concepts/defaults.md).

## Record layout

| Record | Key | Bins |
|---|---|---|
| Shard configuration | `<magazine>_SHARDS` | `SHARDS`, `META_VERSION`, `CREATED_AT` |
| Metadata (unified) | `<magazine>_SHARD_<n>_METADATA` | `LOAD_POINTER`, `FIRE_POINTER`, `LOAD_COUNTER`, `FIRE_COUNTER` |
| Metadata (legacy) | `<magazine>_SHARD_<n>_POINTERS` and `..._COUNTERS` | pointers / counters split across two records |
| Data | `<magazine>_SHARD_<n>_<pointer>` | `data` |
| Dedupe marker | `<magazine><payload>` in `<clientId>_deduper` | `modified_at` |

Unsharded magazines (`shards <= 1`) omit the `SHARD_<n>` fragment entirely. That is why an
unsharded magazine holding undelivered records can never be promoted to sharded — the flat keys
would become unreachable.

## Initialisation

On construction each `Magazine` resolves its persisted configuration once, into an immutable
context that is passed to every subsequent call. A storage instance holds no per-magazine state and
can serve many magazines.

1. Read `<magazine>_SHARDS`.
2. If absent, create it with `CREATE_ONLY` at `META_VERSION=2` and a 5-year TTL. A concurrent
   creator is resolved by re-reading.
3. If present, reconcile the configured shard count against the persisted one. **The persisted
   count wins** unless `allowShardIncrease` is set — see [Defaults](../concepts/defaults.md).
4. `META_VERSION=0` (or absent) selects the legacy split layout; `2` selects unified.

## Load

1. If deduplication is on, claim the marker with a `CREATE_ONLY` write. `KEY_EXISTS_ERROR` means
   the payload was already loaded — return `true` without writing.
2. Pick a random shard and atomically increment `LOAD_POINTER`, taking the value it returns.
3. Write the payload at that pointer.
4. Increment `LOAD_COUNTER`.

Three round trips, or four with deduplication. The counter is published *after* the write, which is
what makes holes detectable: a failed step 3 leaves `LOAD_POINTER` ahead of `LOAD_COUNTER`.

If the load does not complete, the deduplication marker is withdrawn so a retry is not suppressed
until the TTL elapses.

## Fire

1. Pick a random shard from the cached active set.
2. Atomically increment `FIRE_POINTER`, guarded by a filter asserting
   `FIRE_POINTER < LOAD_POINTER`, and read back the value produced.
3. Read the payload at that pointer.
4. Increment `FIRE_COUNTER`.

Three round trips, **regardless of how many consumers are running**. Because every caller
increments rather than compare-and-swapping an expected value, concurrent consumers receive
distinct pointers and a claim can never be lost. There is no contention budget and no backoff.

A filtered-out claim means the shard is drained; it is pruned from the cached active set, and once
every shard is pruned `fire()` raises `NOTHING_TO_FIRE`.

If step 3 finds nothing, the slot is a **hole** — its write had failed. `fire()` skips it and
continues, up to `maxFireHoleSkips`.

The claim is never retried. See [delivery semantics](../concepts/delivery-semantics.md).

### Active shard discovery

Which shards hold data is cached per magazine and refreshed every `activeShardRefreshSeconds`
(default 5) on access, so idle magazines cost nothing. Each refresh is one batch read across every
shard.

A shard counts as active only when **both** ledgers agree:

```
LOAD_POINTER > FIRE_POINTER   and   LOAD_COUNTER > FIRE_COUNTER
```

Both clauses are required and it is not redundant. The pointer clause answers *"are there
unconsumed slots?"*; the counter clause answers *"does any of them hold real data?"*. They diverge
by exactly the number of holes. Without the counter clause, `fire()` would burn its hole-skip
budget on shards containing nothing but failed writes.

The ledger is safe because `FIRE_COUNTER` can never over-count — it advances only after a payload
was actually read, so a crash leaves it short, which keeps the shard active and fails open.

## Metadata schema versions

`META_VERSION=2` keeps pointers and counters in one record; `0` splits them. Both are supported and
**there is no migration** — magazines are expected to be short-lived, so legacy ones simply drain
and disappear.

Legacy magazines cost one extra batch read per active-shard refresh and per metadata read, because
counters live in a separate record.

!!! note "Legacy accounting"
    Under either schema, a crash between claiming the pointer and advancing `FIRE_COUNTER` leaves
    the counter short. That only inflates the `pending` figure on the dashboard; it cannot strand
    data. It resolves as the magazine drains.

## Error mapping

| Cause | `ErrorCode` |
|---|---|
| All retries failed on a storage call | `RETRIES_EXHAUSTED` |
| Hole-skip budget spent | `RETRIES_EXHAUSTED` |
| Thread interrupted (flag restored) | `RETRIES_EXHAUSTED` |
| Metadata record missing | `MAGAZINE_UNPREPARED` |
| Every shard drained | `NOTHING_TO_FIRE` |
| Non-retryable storage failure | `CONNECTION_ERROR` |
| Payload type mismatch | `DATA_TYPE_MISMATCH` |
| Shard layout change refused | `INVALID_SHARDS` |

## Operational notes

- **Set names.** `LOCAL` scope prefixes data and metadata sets with `farmId`, so two farms sharing a
  namespace do not collide.
- **TTLs.** Metadata TTL must exceed record TTL; enforced at construction. A magazine idle for
  longer than `metaDataTtl` loses its metadata and reports `MAGAZINE_UNPREPARED`.
- **Client tuning.** Magazine copies your client's default read, write and batch policies once at
  construction, then reuses them. Configure timeouts and retries on the `IAerospikeClient` you pass
  in.
- **Rolling deploys.** Old and new versions can run against the same magazine. Shard counts are
  never rewritten unless `allowShardIncrease` is set, so a mixed fleet with differing `shards`
  config converges on the persisted value rather than fighting.
