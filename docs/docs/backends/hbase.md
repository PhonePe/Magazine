# HBase Backend

!!! warning "Not Yet Implemented"
    The HBase backend is a placeholder for future development. All operations currently throw `UnsupportedOperationException`.

## Overview

`HBaseStorage<T>` extends `BaseMagazineStorage<T>` with `StorageType.HBASE`. It is included to define the interface contract for a future HBase-backed magazine implementation.

## Construction

```java
HBaseStorage<String> storage = new HBaseStorage<>(
        recordTtl,          // int — TTL in seconds for data records
        metaDataTtl,        // int — TTL in seconds for metadata records
        farmId,             // String — data centre identifier
        deDupeEnabled,      // boolean — enable de-duplication
        shards,             // int — number of shards
        clientId,           // String — owning service identifier
        MagazineScope.LOCAL // MagazineScope — LOCAL or GLOBAL
);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `recordTtl` | `int` | TTL in seconds for data records. |
| `metaDataTtl` | `int` | TTL in seconds for metadata records. |
| `farmId` | `String` | Data centre / farm identifier. |
| `deDupeEnabled` | `boolean` | Whether de-duplication is enabled. |
| `shards` | `int` | Number of shards. |
| `clientId` | `String` | Owning service identifier. |
| `scope` | `MagazineScope` | `LOCAL` or `GLOBAL`. |

## Supported Operations

| Method | Status |
|--------|--------|
| `load(String, T)` | :x: `UnsupportedOperationException` |
| `reload(String, T)` | :x: `UnsupportedOperationException` |
| `fire(String)` | :x: `UnsupportedOperationException` |
| `getMetaData(String)` | :x: `UnsupportedOperationException` |
| `delete(MagazineData<T>)` | :x: `UnsupportedOperationException` |
| `peek(String, Map<Integer, Set<Long>>)` | :x: `UnsupportedOperationException` |

## Contributing an HBase Implementation

If you are interested in contributing an HBase backend, please:

1. Extend `BaseMagazineStorage<T>` with `StorageType.HBASE`.
2. Implement all six abstract methods.
3. Add the `StorageTypeVisitor.visitHBase()` validation logic in `Magazine.validateStorage()`.
4. Add integration tests using Testcontainers with an HBase container.
5. Open a pull request — see [CONTRIBUTING.md](https://github.com/PhonePe/Magazine/blob/main/CONTRIBUTING.md) for guidelines.

