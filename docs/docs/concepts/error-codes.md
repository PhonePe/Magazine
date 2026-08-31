# Error Codes

All errors in Magazine are surfaced as `MagazineException`, which extends `RuntimeException`. Each exception carries an `ErrorCode` enum value that describes the failure category.

## Exception Structure

```java
public class MagazineException extends RuntimeException {
    private final ErrorCode errorCode;

    // Builder pattern via Lombok
    @Builder
    public MagazineException(ErrorCode errorCode, String message, Throwable cause) { ... }

    // Unwrap nested MagazineExceptions
    public static MagazineException propagate(Throwable throwable) { ... }
    public static MagazineException propagate(String message, Throwable throwable) { ... }
}
```

## Error Code Reference

| Error Code | When It Occurs |
|------------|----------------|
| `CONNECTION_ERROR` | Unable to connect to the storage backend. |
| `INTERNAL_ERROR` | Catch-all for unexpected failures (I/O errors, serialization issues, etc.). Also used when wrapping unknown exceptions via `propagate()`. |
| `RETRIES_EXHAUSTED` | All retry attempts for a storage operation have been exhausted. |
| `MAGAZINE_UNPREPARED` | Metadata (pointers or counters) could not be read — the magazine may not have been initialised. |
| `NOTHING_TO_FIRE` | There is no data available to fire (all items have been consumed). |
| `MAGAZINE_NOT_FOUND` | `MagazineManager.getMagazine()` was called with an identifier that has not been registered via `refresh()`. |
| `ACTION_DENIED_PARALLEL_ATTEMPT` | A concurrent operation was attempted while a distributed lock was held (de-duplication conflict). |
| `NOT_IMPLEMENTED` | The requested operation is not supported, such as `GLOBAL` scope. |
| `INVALID_SHARDS` | Invalid shard configuration: cannot decrease shards, or cannot convert unsharded to sharded. |
| `DATA_TYPE_MISMATCH` | The data type does not match the expected type for the magazine. |

## Error Messages

Pre-defined error message templates are available in `com.phonepe.magazine.util.ErrorMessage`:

| Constant | Template |
|----------|----------|
| `ERROR_LOADING_DATA` | `"Error loading data [magazineIdentifier = %s]"` |
| `ERROR_GETTING_META_DATA` | `"Error getting meta data [magazineIdentifier = %s]"` |
| `ERROR_FIRING_DATA` | `"Error firing data [magazineIdentifier = %s]"` |
| `ERROR_READING_POINTERS` | `"Error reading pointers [magazineIdentifier = %s]"` |
| `ERROR_READING_COUNTERS` | `"Error reading counters [magazineIdentifier = %s]"` |
| `NO_DATA_TO_FIRE` | `"No data to fire [magazineIdentifier = %s]"` |
| `ERROR_DELETING_DATA` | `"Error deleting data [magazineIdentifier = %s]"` |
| `ERROR_PEEKING_DATA` | `"Error peeking data [magazineIdentifier = %s]"` |

## Exception Propagation

`MagazineException.propagate()` unwraps nested exceptions to preserve the original error code:

```java
try {
    magazine.load(data);
} catch (MagazineException e) {
    // e.getErrorCode() gives you the specific failure reason
    switch (e.getErrorCode()) {
        case CONNECTION_ERROR -> reconnect();
        case RETRIES_EXHAUSTED -> backoff();
        case ACTION_DENIED_PARALLEL_ATTEMPT -> log.warn("Duplicate write blocked");
        case INVALID_SHARDS -> throw e; // configuration error, fail fast
        default -> log.error("Unexpected error", e);
    }
}
```

If the thrown exception is already a `MagazineException` (or its cause is), `propagate()` returns it directly. Otherwise, it wraps the exception with `INTERNAL_ERROR`.
