# Dropwizard Bundle And Dashboard

`magazine-dw-bundle` is the Dropwizard 5 integration for Magazine. It currently registers read-only Magazine APIs and an optional responsive dashboard, and its `MagazineBundle` registration point can be extended with future APIs.

## Dependency

```xml
<dependency>
  <groupId>com.phonepe</groupId>
  <artifactId>magazine-dw-bundle</artifactId>
  <version>${magazine.version}</version>
</dependency>
```

Applications using only `com.phonepe:magazine-core` do not acquire Dropwizard dependencies.

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
```

Registering `MagazineBundle` always enables the Magazine APIs. `dashboardEnabled` controls only the static dashboard and defaults to `true`.

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

The bundle creates and exposes its `MagazineManager`, following the Ignis bundle pattern. `MagazineManager.refresh(...)` atomically replaces the registered map, and API/dashboard requests see the new magazine set immediately without a restart. Identifiers may contain letters, digits, `.`, `_`, or `-`.

For future APIs, extend `registerResources(...)`, call `super.registerResources(...)`, and register additional resources. This keeps registration centralized in the bundle instead of dashboard-specific code.

## Dashboard

The bundle mirrors Arc's asset setup:

- Classpath assets: `/magazineAssets/`
- Servlet path: `/magazineDashboard/*`
- Index asset: `magazineIndex.html`
- Browser URL: `http://localhost:8080/magazineDashboard/`

The peek panel accepts a shard number and pointer directly. Users do not need to write JSON.

Metadata refreshes automatically every 30 seconds for the selected magazine. The header displays the last successful refresh time. Load/fire counters and load/fire pointers are shown separately in the per-shard rows. Only counters and `pending` are aggregated in the totals: pointers are per-shard monotonic sequences, so summing them across shards produces a number with no meaning, and the totals report them as `0`.

### Screenshot

Add the dashboard screenshot at `docs/docs/assets/magazine-dashboard.png`. After adding it, insert `![Magazine dashboard](assets/magazine-dashboard.png)` here.

## Routes

| Method | Route | Behavior |
|---|---|---|
| `GET` | `/magazineDashboard/` | Static dashboard |
| `GET` | `/magazineDashboard/style.css` | Static stylesheet |
| `GET` | `/magazine/v1/magazines` | Configured magazines |
| `GET` | `/magazine/v1/magazines/{identifier}/metadata` | Per-shard and total metadata |
| `POST` | `/magazine/v1/magazines/{identifier}/peek` | Non-consuming record read |

The API still accepts batched programmatic peek requests:

```json
{
  "pointers": {
    "0": ["1", "2"]
  }
}
```

Counters and pointers are returned as JSON numbers. Each peeked record also carries a `type` field with the payload's simple class name; a payload that cannot be serialised degrades to its `toString()` rather than failing the request.

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
