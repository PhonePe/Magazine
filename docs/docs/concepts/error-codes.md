# Error Codes

Every failure Magazine raises is a `MagazineException` carrying an `ErrorCode`. Catch the
exception and switch on the code; the message is for humans, the code is the contract.

## Codes

| Code | Meaning |
|---|---|
| `CONNECTION_ERROR` | The storage backend could not be reached, or failed in a way not worth retrying. |
| `INTERNAL_ERROR` | An unexpected failure that does not map to any other code. |
| `RETRIES_EXHAUSTED` | Gave up. **Not** the same as an empty queue — see below. |
| `MAGAZINE_UNPREPARED` | Metadata for the magazine is missing. Either it was never initialised, or its metadata record expired or was deleted. |
| `NOTHING_TO_FIRE` | Every shard is drained. This is the normal, expected way a consumer learns the queue is empty. |
| `MAGAZINE_NOT_FOUND` | No magazine is registered under that identifier in the `MagazineManager`. |
| `NOT_IMPLEMENTED` | A configured capability is not implemented — currently only `MagazineScope.GLOBAL`. Raised at construction, so the application fails at boot. |
| `INVALID_SHARDS` | A shard count is out of range, or a shard-layout change was refused. |
| `INVALID_CONFIGURATION` | A configuration value is missing or invalid. Raised at construction. |
| `DATA_TYPE_MISMATCH` | The payload does not match the magazine's declared type, or deduplication was enabled for an unsupported type. |

## `NOTHING_TO_FIRE` vs `RETRIES_EXHAUSTED`

These mean opposite things and a consumer loop must treat them differently.

- **`NOTHING_TO_FIRE`** — every shard was checked and every shard is drained. There is no data.
  This is your loop's terminating condition.
- **`RETRIES_EXHAUSTED`** — Magazine gave up before establishing that. **Data may still exist.**
  Back off and try again; do not treat it as an empty queue.

`fire()` raises `RETRIES_EXHAUSTED` in three situations:

1. The hole-skip budget (`maxFireHoleSkips`, default 512) was spent. A *hole* is a pointer slot
   that was allocated but whose data write failed; `fire()` skips them, but not forever.
2. A storage call failed on every attempt (`MAX_RETRIES` = 5).
3. **The thread was interrupted.** The interrupt flag is restored before the exception is thrown.

That third case matters: a loop that catches `RETRIES_EXHAUSTED` and sleeps will spin on an
interrupted thread. Check the interrupt flag.

```java
while (!Thread.currentThread().isInterrupted()) {
    try {
        MagazineData<String> record = magazine.fire();
        process(record.getData());
    } catch (MagazineException e) {
        switch (e.getErrorCode()) {
            case NOTHING_TO_FIRE -> sleep(idleBackoff);
            case RETRIES_EXHAUSTED, CONNECTION_ERROR -> sleep(errorBackoff);
            case MAGAZINE_UNPREPARED -> {
                log.error("Magazine metadata is gone", e);
                return;
            }
            default -> throw e;
        }
    }
}
```

## `MAGAZINE_UNPREPARED`

Metadata TTL is validated at construction to outlive record TTL, so a missing metadata record
means the data is gone too. Magazine surfaces this rather than quietly reporting an empty queue,
because the two need very different responses.

The usual causes are a magazine that was never loaded into, or a `metaDataTtl` that elapsed on an
idle magazine.

## Error messages

Message templates live in `com.phonepe.magazine.impl.aerospike.common.ErrorMessage`. They are an
implementation detail of the Aerospike backend — match on `ErrorCode`, never on message text.
