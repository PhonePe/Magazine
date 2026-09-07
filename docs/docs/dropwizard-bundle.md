# Dropwizard Bundle And Dashboard

`magazine-dw-bundle` is the Dropwizard 5 integration for Magazine. It registers read-only Magazine APIs and an optional responsive dashboard.

## Dependency

```xml
<dependency>
  <groupId>com.phonepe</groupId>
  <artifactId>magazine-dw-bundle</artifactId>
  <version>${magazine.version}</version>
</dependency>
```

Applications using only `com.phonepe:magazine-core` do not acquire Dropwizard dependencies.

## Dropwizard version compatibility

| Dropwizard | `magazine-core` | `magazine-dw-bundle` |
|---|---|---|
| 5.x | ✅ | ✅ |
| 4.x | ✅ | ⚠️ likely — same `jakarta` namespace, untested |
| 3.x | ✅ | ⚠️ likely — same `jakarta` namespace, untested |
| 2.x | ✅ (Java 17+ only) | ❌ **not possible** |

### Why 2.x cannot work

This is not a version skew that a dependency override can bridge. Dropwizard 3 moved the whole
stack from `javax.*` to `jakarta.*`, and the bundle is compiled against the latter:

| Bundle uses | Dropwizard 2.x has |
|---|---|
| `jakarta.ws.rs.*` | `javax.ws.rs.*` |
| `jakarta.annotation.security.RolesAllowed` | `javax.annotation.security.RolesAllowed` |
| `io.dropwizard.core.ConfiguredBundle` | `io.dropwizard.ConfiguredBundle` |
| Jersey 3 `RolesAllowedDynamicFeature` | Jersey 2 equivalent |

Different packages are different classes. `MagazineBundle` would not load, and neither would the
resource. Byte-code relocation of `javax` → `jakarta` is theoretically possible and not something
to run in production.

### What a Dropwizard 2.x service can do

**Use `magazine-core` directly.** It has no framework dependency at all — its compile graph is
`aerospike-client`, `caffeine`, `slf4j` and `micrometer-core` — so the queue itself works
unchanged. Only the ~200-line read-only HTTP layer is unavailable.

!!! warning "Java 17 required"
    `magazine-core` targets Java 17. Dropwizard 2.1.x supports Java 11, so a service still on 11
    cannot use Magazine 2.0 at all and must stay on 1.x until it upgrades.

Rebuilding the endpoints is small. `MagazineManager` and `Magazine` are plain objects:

```java
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/magazines")
@Produces(MediaType.APPLICATION_JSON)
public class MagazineAdminResource {

    private final MagazineManager magazineManager;

    public MagazineAdminResource(MagazineManager magazineManager) {
        this.magazineManager = magazineManager;
    }

    @GET
    public Set<String> magazines() {
        return magazineManager.getMagazineMap().keySet();
    }

    @GET
    @Path("/{identifier}/metadata")
    public Map<String, MetaData> metadata(@PathParam("identifier") String identifier) {
        return magazineManager.getMagazine(identifier).getMetaData();
    }

    @POST
    @Path("/{identifier}/peek")
    @RolesAllowed("magazine_peek")
    public Set<? extends MagazineData<?>> peek(@PathParam("identifier") String identifier,
                                               Map<Integer, Set<Long>> pointers) {
        return magazineManager.getMagazine(identifier).peek(pointers);
    }
}
```

Two things the bundle does that you should copy:

- **Bound the peek.** The bundle caps a request at 1000 pointers. An unbounded peek is a way to ask
  the cluster for everything at once.
- **Fail closed on peek.** Peek reads payloads. Register `RolesAllowedDynamicFeature` and make sure
  whatever populates the `SecurityContext` is annotated `@Priority(Priorities.AUTHENTICATION)`, or
  the role check runs first and denies everything.

Caching the metadata response for a few seconds is also worth copying — see `metadataCacheSeconds`
below for why.

## Configuration

```java
@Getter
@Setter
public final class ServiceConfiguration extends Configuration {
    @Valid
    @NotNull
    private MagazineBundleConfiguration magazineBundle = new MagazineBundleConfiguration();

}
```

```yaml
magazineBundle:
  dashboardEnabled: true
  metadataCacheSeconds: 5
  metricsEnabled: true
```

Registering `MagazineBundle` always enables the Magazine APIs. `dashboardEnabled` controls only the static dashboard and defaults to `true`.

| Field | Default | Description |
|---|---:|---|
| `dashboardEnabled` | `true` | Serve the static dashboard assets. |
| `metadataCacheSeconds` | `5` | Seconds to cache a magazine's shard metadata for; `0` disables caching. |
| `metricsEnabled` | `true` | Publish Magazine's metrics through the application's metric registry, so they appear on the admin port. |

`metricsEnabled` bridges Magazine's meters to `environment.metrics()` and attaches that bridge to Micrometer's global registry. Storages need no wiring — build them without a `meterRegistry` and their metrics land on the admin port alongside the rest of the service's. Override `createMeterRegistry(...)` to publish elsewhere. See [Metrics](concepts/metrics.md).

`metadataCacheSeconds` throttles the per-shard fan-out behind `/metadata`. Rendering the dashboard costs one batch read per magazine that fans out to every shard, so an open dashboard — or anything polling `/metadata` on a timer — issues `magazines x shards` key reads per refresh against the same cluster serving production traffic. Caching collapses that to at most one fan-out per magazine per window. An unknown magazine is resolved before the cache is consulted, so it still returns `404` rather than being cached as a miss.

## Registration

Follow the Arc/Ignis bundle pattern by extending `MagazineBundle` and supplying application-owned Magazine instances:

```java
public final class ServiceMagazineBundle extends MagazineBundle<ServiceConfiguration> {
    @Override
    protected MagazineBundleConfiguration getMagazineBundleConfiguration(
            ServiceConfiguration configuration) {
        return configuration.getMagazineBundle();
    }

    @Override
    protected String getClientId(ServiceConfiguration configuration) {
        return configuration.getClientId();
    }
}
```

```java
private final ServiceMagazineBundle magazineBundle = new ServiceMagazineBundle();

@Override
public void initialize(Bootstrap<ServiceConfiguration> bootstrap) {
    bootstrap.addBundle(magazineBundle);
}

@Override
public void run(ServiceConfiguration configuration, Environment environment) {
    magazineBundle.getMagazineManager().refresh(configuredMagazines);
}
```

The bundle creates and exposes its `MagazineManager`, following the Ignis bundle pattern. `MagazineManager.refresh(...)` atomically replaces the registered map, and API/dashboard requests see the new magazine set immediately without a restart.

`MagazineBundle` declares exactly two abstract methods, both shown above:

| Method | Purpose |
|---|---|
| `getMagazineBundleConfiguration(T configuration)` | Return the bundle's `MagazineBundleConfiguration`. Must not be null. |
| `getClientId(T configuration)` | Return the owning client identifier used to build the `MagazineManager`. Must not be null. |

There is no resource-registration extension point; the bundle registers `MagazineResource` and `RolesAllowedDynamicFeature` itself. Register any additional resources of your own directly on `environment.jersey()`.

## Dashboard

The bundle mirrors Arc's asset setup:

- Classpath assets: `/magazineAssets/`
- Servlet path: `/magazineDashboard/*`
- Index asset: `magazineIndex.html`
- Browser URL: `http://localhost:8080/magazineDashboard/`

The peek panel accepts a shard number and pointer directly. Users do not need to write JSON.

Metadata refreshes automatically every 30 seconds for the selected magazine. The header displays the last successful refresh time. Load/fire counters and load/fire pointers are shown separately in the per-shard rows. Only counters and `pending` are aggregated in the totals: pointers are per-shard monotonic sequences, so summing them across shards produces a number with no meaning, and the totals report them as `0`.

### Screenshot

_Screenshot pending._

## Routes

| Method | Route | Behavior |
|---|---|---|
| `GET` | `/magazineDashboard/` | Static dashboard |
| `GET` | `/magazineDashboard/style.css` | Static stylesheet |
| `GET` | `/magazine/v1/magazines` | Configured magazines |
| `GET` | `/magazine/v1/magazines/{identifier}/metadata` | Per-shard and total metadata |
| `POST` | `/magazine/v1/magazines/{identifier}/peek` | 🔒 Non-consuming record read — requires role `magazine_peek` |

### Metadata response

```json
{
  "identifier": "email-jobs",
  "shards": {
    "SHARD_0": {
      "loadCounter": 36,
      "fireCounter": 34,
      "loadPointer": 36,
      "firePointer": 34,
      "pending": 2
    },
    "SHARD_1": {
      "loadCounter": 31,
      "fireCounter": 30,
      "loadPointer": 31,
      "firePointer": 30,
      "pending": 1
    }
  },
  "totals": {
    "loadCounter": 67,
    "fireCounter": 64,
    "loadPointer": 0,
    "firePointer": 0,
    "pending": 3
  }
}
```

`loadPointer` and `firePointer` are deliberately reported as `0` in `totals` — see the note above.

The API still accepts batched programmatic peek requests:

```json
{
  "pointers": {
    "0": [1, 2]
  }
}
```

### Peek response

```json
{
  "identifier": "email-jobs",
  "data": [
    {
      "shard": 0,
      "pointer": 1,
      "type": "String",
      "data": "user:1001:welcome"
    },
    {
      "shard": 0,
      "pointer": 2,
      "type": "String",
      "data": "user:1002:shipped"
    }
  ]
}
```

`shard` is `null` for an unsharded magazine. Counters and pointers are returned as JSON numbers, never strings. Each peeked record also carries a `type` field with the payload's simple class name; a payload that cannot be serialised degrades to its `toString()` rather than failing the request.

Programmatic peek requests are limited to 1,000 pointers per request. This fixed guard is not configurable.

## Read-Only Guarantee

The HTTP layer has no `load`, `reload`, `fire`, or deletion routes. It does not expose `BaseMagazineStorage`. Metadata uses `Magazine#getMetaData()`, and peek uses only `Magazine#peek()` without changing pointers, counters, or records.

## Securing Peek

Listing and metadata expose counters only and are unauthenticated. **Peek returns complete
payloads and is guarded by `@RolesAllowed("magazine_peek")`.** The bundle registers Jersey's
`RolesAllowedDynamicFeature` and provides no authentication of its own, so with no `SecurityContext`
in play the role check fails and peek is closed by default.

Enable it by registering your own authentication and granting the role
(`MagazineResource.PEEK_ROLE`):

```java
environment.jersey().register(new AuthDynamicFeature(
        new OAuthCredentialAuthFilter.Builder<Operator>()
                .setAuthenticator(authenticator)
                .setAuthorizer((principal, role, ctx) ->
                        principal.roles().contains(role))   // grant "magazine_peek" here
                .setPrefix("Bearer")
                .buildAuthFilter()));
```

Any custom filter that populates the `SecurityContext` must be annotated
`@Priority(Priorities.AUTHENTICATION)`. `RolesAllowedDynamicFeature` evaluates at
`Priorities.AUTHORIZATION`, so a filter left at the default user priority runs *after* the role
check has already failed.

Peek being gated is not sufficient on its own. Also:

- Protect `/magazineDashboard/*` and `/magazine/v1/*` with application authentication.
- Restrict the application connector through network policy.
- Include only approved Magazine instances.
- Configure a conservative Dropwizard request-body limit.

## Local Demo

Run the test-only demo without Aerospike or Docker:

```bash
mvn -pl magazine-dw-bundle -am -Pdashboard-demo test-compile
```

Open `http://localhost:8080/magazineDashboard/` and stop it with `Ctrl+C`.

| Magazine | Shard | Pointers |
|---|---:|---|
| `email-jobs` | `0` | `35`, `36` |
| `email-jobs` | `1` | `31` |
| `payment-retries` | `0` | `50`, `51` |
| `payment-retries` | `1` | `47` |

The demo is compiled from `src/test` and is excluded from the published JAR.

## Build And Test

```bash
mvn clean package
```

Core integration tests require Docker. Bundle tests cover registration, static assets, metadata, bounded peek, non-mutating behavior, absent mutating routes, and the standalone demo server.
