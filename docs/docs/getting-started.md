# Getting Started

## Requirements

- **Java 17** or later.
- One of the supported storage backends:
    - An **Aerospike** cluster reachable from application nodes.
    - An **HBase** cluster (planned — not yet implemented).
- **Docker** (for running integration tests via Testcontainers).

## Add Dependency

```xml
<dependency>
  <groupId>com.phonepe</groupId>
  <artifactId>magazine</artifactId>
  <version>${magazine.version}</version>
</dependency>
```

Replace `${magazine.version}` with the latest version from [Maven Central](https://central.sonatype.com/artifact/com.phonepe/magazine) or [GitHub Releases](https://github.com/PhonePe/Magazine/releases).

## Build Locally

```bash
git clone https://github.com/PhonePe/Magazine.git
cd Magazine
mvn clean install
```

To run the tests (Docker must be running):

```bash
mvn clean test
```

## Minimal Example

### 1. Connect to Aerospike

```java
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;

IAerospikeClient client = new AerospikeClient("localhost", 3000);
```

### 2. Configure and Build Storage

```java
import com.phonepe.magazine.impl.aerospike.AerospikeStorage;
import com.phonepe.magazine.impl.aerospike.AerospikeStorageConfig;
import com.phonepe.magazine.scope.MagazineScope;

AerospikeStorageConfig config = AerospikeStorageConfig.builder()
        .namespace("test")
        .dataSetName("magazine_data")
        .metaSetName("magazine_meta")
        .shards(64)
        .recordTtl(30 * 24 * 60 * 60)       // 30 days
        .metaDataTtl(2 * 30 * 24 * 60 * 60)  // 60 days
        .build();

AerospikeStorage<String> storage = AerospikeStorage.<String>builder()
        .aerospikeClient(client)
        .storageConfig(config)
        .enableDeDupe(false)
        .farmId("dc1")
        .clazz(String.class)
        .clientId("my-service")
        .scope(MagazineScope.LOCAL)
        .build();
```

### 3. Create a Magazine

```java
import com.phonepe.magazine.Magazine;

Magazine<String> magazine = Magazine.<String>builder()
        .baseMagazineStorage(storage)
        .magazineIdentifier("notifications")
        .build();
```

### 4. Load, Fire, Inspect

```java
// Load data
magazine.load("Hello, Magazine!");

// Fire (consume) next item
MagazineData<String> fired = magazine.fire();
System.out.println(fired.getData()); // "Hello, Magazine!"

// Inspect metadata
Map<String, MetaData> meta = magazine.getMetaData();
meta.forEach((shard, m) ->
    System.out.printf("Shard %s → loaded=%d fired=%d%n",
        shard, m.getLoadCounter(), m.getFireCounter()));
```

!!! tip
    Use `MagazineManager` when you need to manage multiple magazines of different types in one place.
    See [Usage](usage.md) for a complete example.

## What's Next

- [Usage](usage.md) — all operations with tabbed backend examples.
- [API Reference](concepts/api-reference.md) — full method documentation.
- [Storage Backends](backends/aerospike.md) — Aerospike configuration deep-dive.

