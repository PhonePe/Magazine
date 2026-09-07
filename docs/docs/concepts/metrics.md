# Metrics

Magazine publishes [Micrometer](https://micrometer.io) metrics.

You do not pass a registry to each storage. A storage built without one publishes to Micrometer's
**global registry**, so you wire your backend once for the whole application:

```java
Metrics.addRegistry(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));

AerospikeStorage.<String>builder()
    .storageConfig(config)            // metricsEnabled defaults to true
    // ...
    .build();                         // no meterRegistry needed
```

The global registry is a composite, so **attach order does not matter** — a registry added after a
storage was built still receives that storage's meters. (Increments recorded before the attach are
not backfilled, which is why you attach at startup.)

Two ways to deviate:

- **Pass `.meterRegistry(registry)`** — that storage publishes there instead of the global registry.
  Use it when you want isolation, typically in tests.
- **`metricsEnabled=false`** — that storage publishes to a private empty registry, so nothing is
  recorded and attaching a backend globally later cannot switch it back on.

There is no separate "disabled" code path: switching metrics off is just a registry with nothing
attached, whose counters are no-ops.

## With the Dropwizard bundle

`magazine-dw-bundle` bridges Magazine's meters to the application's Dropwizard metric registry and
attaches that bridge to the global registry. Metrics appear on the **admin port** next to everything
else the service reports, and **you wire nothing** — adding the bundle is the whole setup.

```java
public class MyApp extends Application<MyConfig> {

    private final MagazineBundle<MyConfig> magazineBundle = new MagazineBundle<>() { /* ... */ };

    @Override
    public void run(MyConfig config, Environment environment) {
        AerospikeStorage<String> storage = AerospikeStorage.<String>builder()
                // no meterRegistry: the bundle already attached one globally
                .build();

        magazineBundle.getMagazineManager().refresh(List.of(
                Magazine.<String>builder()
                        .magazineIdentifier("email-jobs")
                        .baseMagazineStorage(storage)
                        .build()));
    }
}
```

This works because the bundle's `run` executes before the application's — the same ordering that
makes `getMagazineManager()` usable there. `getMeterRegistry()` is still exposed if you want to hand
a storage the bridge explicitly.

The attachment is undone on shutdown, so a second bundle in the same JVM — a test suite, typically
— does not inherit the first one's backend.

Disable with `metricsEnabled: false` under `magazineBundle` in your YAML. The bundle then attaches
nothing globally, so the opt-out cannot be undone by another component attaching a registry.

### Flattened names

Dropwizard's registry is hierarchical, not dimensional, so tags become name segments:

```
magazine.aerospike.calls.magazine.email-jobs.operation.claim_fire_pointer
magazine.fire.claims.magazine.email-jobs.outcome.won
magazine.fire.latency.magazine.email-jobs
```

Nothing is lost, but a Grafana query matches on the flattened name rather than on a label. The
`magazine` and `operation`/`outcome` segments are still there, so
`magazine.aerospike.calls.magazine.*.operation.claim_fire_pointer` gives you the claim rate across
every magazine.

Names keep their dots deliberately — Dropwizard's default convention would camel-case
`magazine.aerospike.calls` into `magazineAerospikeCalls`, which would not match the names in this
document.

To publish somewhere else instead — Prometheus, OTLP — override `createMeterRegistry`. Whatever you
return is attached globally, so storages still need no wiring:

```java
@Override
protected MeterRegistry createMeterRegistry(MyConfig config, Environment environment) {
    return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
}
```

## What is published

All meters are tagged `magazine`.

| Meter | Type | Tags | What it tells you |
|---|---|---|---|
| `magazine.aerospike.calls` | counter | `operation` | **Round trips**, not logical operations. The most useful metric here. |
| `magazine.fire.claims` | counter | `outcome` = `won` \| `drained` | `drained` means the claim found the shard empty. |
| `magazine.fire.hole.skips` | counter | — | Slots skipped because the data write had failed. Sustained non-zero means writes are failing. |
| `magazine.fire.outcomes` | counter | `outcome` = `delivered` \| `empty` \| `exhausted` \| `failed` | |
| `magazine.load.outcomes` | counter | `outcome` = `loaded` \| `duplicate` \| `failed` | |
| `magazine.dedupe.outcomes` | counter | `outcome` = `claimed` \| `duplicate` | Duplicate suppression rate. |
| `magazine.fire.latency` | timer | — | End-to-end, including hole skips. |
| `magazine.load.latency` | timer | — | End-to-end, including deduplication. |

### `operation` values

`claim_fire_pointer`, `increment_load_pointer`, `increment_load_counter`, `increment_fire_counter`,
`decrement_fire_counter`, `batch_read_metadata`, `refresh_active_shards`, `read_data`, `write_data`,
`delete_data`, `batch_read_data`, `claim_dedupe_marker`, `withdraw_dedupe_marker`.

`refresh_active_shards` is the batch read behind an active-shard cache refresh, split out from
`batch_read_metadata` so refresh traffic is attributable without a meter of its own. A magazine on
the split metadata schema issues two per refresh, one per ledger — these count round trips, not
refreshes.

## Expected round trips

Divide `magazine.aerospike.calls` by the matching outcome counter. Healthy steady state:

| Operation | Calls | Breakdown |
|---|---|---|
| `fire()` | **3** | claim, read payload, advance counter |
| `load()` | **3** | allocate pointer, write payload, advance counter |
| `load()` with dedupe | **4** | + claim marker |
| `reload()` | **3** | allocate pointer, write payload, decrement fire counter |
| Shard discovery | 1–2 batches / `activeShardRefreshSeconds` / magazine | `refresh_active_shards`; fans out to every shard |

**Calls per `fire()` does not rise with consumer count.** The claim is a guarded atomic increment,
so concurrent consumers get distinct pointers and no claim is ever lost. If you see the ratio climb
above 3, look at `magazine.fire.hole.skips` — the cause is failed writes, not contention.

## What to alert on

| Signal | Meaning |
|---|---|
| `fire.outcomes{outcome=exhausted}` > 0 | Giving up with data possibly present. Investigate. |
| `fire.hole.skips` sustained | Data writes are failing; pointers are being allocated for records that never land. |
| `fire.claims{outcome=drained}` ≫ `won` | Consumers are chasing empty shards. Usually more consumers than work. |
| `aerospike.calls` ÷ `fire.outcomes{delivered}` ≫ 3 | Hole skipping. |
| `aerospike.calls{operation=refresh_active_shards}` high | Raise `activeShardRefreshSeconds` or lower `shards`. |

## Cost

Recording is a map lookup and an array index onto a striped counter — no allocation on the hot path.
Against an Aerospike round trip it is well under 0.01%.

Percentile histograms are **not** enabled, because they allocate hundreds of buckets per timer and
do bucket arithmetic on every record. If you want aggregable percentiles, opt in on your own
registry where the cost is explicit:

```java
registry.config().meterFilter(
    new MeterFilter() {
        @Override
        public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
            return id.getName().startsWith("magazine.")
                ? DistributionStatisticConfig.builder().percentilesHistogram(true).build().merge(config)
                : config;
        }
    });
```
