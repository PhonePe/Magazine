package com.phonepe.growth.magazine.impl.aerospike;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.rholder.retry.RetryException;
import com.phonepe.growth.dlm.DistributedLockManager;
import com.phonepe.growth.dlm.exception.DLSException;
import com.phonepe.growth.dlm.lock.Lock;
import com.phonepe.growth.dlm.lock.level.LockLevel;
import com.phonepe.growth.dlm.lock.mode.LockMode;
import com.phonepe.growth.dlm.lock.storage.aerospike.AerospikeLockBase;
import com.phonepe.growth.dlm.lock.storage.aerospike.AerospikeStore;
import com.phonepe.growth.magazine.common.Constants;
import com.phonepe.growth.magazine.common.MagazineData;
import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.StorageType;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import com.phonepe.growth.magazine.util.ErrorMessage;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Getter
@EqualsAndHashCode(callSuper = true)
public class AerospikeStorage<T> extends BaseMagazineStorage<T> {

    public static final String DEDUPER_SET_FORMAT = "%s_deduper";
    private final IAerospikeClient aerospikeClient;
    private final String namespace;
    private final String dataSetName;
    private final String metaSetName;
    private final AerospikeRetryerFactory retryerFactory;
    private final Class<T> clazz;
    private final Random random = new SecureRandom();
    private final AsyncLoadingCache<String, List<String>> activeShardsCache;
    private final DistributedLockManager lockManager;

    @Builder
    public AerospikeStorage(final IAerospikeClient aerospikeClient,
                            final AerospikeStorageConfig storageConfig,
                            final boolean enableDeDupe,
                            final String farmId,
                            final Class<T> clazz,
                            final String clientId) {
        super(StorageType.AEROSPIKE, storageConfig.getRecordTtl(), storageConfig.getMetaDataTtl(),
                farmId, enableDeDupe, storageConfig.getShards(), clientId);
        this.clazz = clazz;
        this.aerospikeClient = aerospikeClient;
        this.namespace = storageConfig.getNamespace();
        this.dataSetName = storageConfig.getDataSetName();
        this.metaSetName = storageConfig.getMetaSetName();
        this.retryerFactory = new AerospikeRetryerFactory();
        this.activeShardsCache = initializeCache();
        this.lockManager = new DistributedLockManager(Constants.DLM_CLIENT_ID, farmId,
                AerospikeLockBase.builder()
                        .mode(LockMode.EXCLUSIVE)
                        .store(AerospikeStore.builder()
                                .aerospikeClient(aerospikeClient)
                                .namespace(namespace)
                                .setSuffix(Constants.MAGAZINE_DISTRIBUTED_LOCK_SET_NAME_SUFFIX)
                                .build())
                        .build());
        lockManager.initialize();
    }

    @Override
    public boolean load(final String magazineIdentifier, final T data) {
        validateDataType(data);
        final Lock lock = lockManager
                .getLockInstance(String.join(Constants.KEY_DELIMITER, magazineIdentifier, data.toString()), LockLevel.DC);
        try {
            // Acquire lock if deDupe is enabled.
            if (isEnableDeDupe()) {
                lockManager.acquire(lock); // Exception is thrown if acquiring lock fails.
            }
            if (!isEnableDeDupe() || (isEnableDeDupe() && !alreadyExists(magazineIdentifier, data))) {
                final Integer selectedShard = selectShard();
                final long loadPointer = incrementAndGetLoadPointer(magazineIdentifier, selectedShard);
                final String key = createKey(magazineIdentifier, selectedShard, String.valueOf(loadPointer));
                final boolean success = loadData(key, data);
                if (success) {
                    incrementLoadCounter(magazineIdentifier, selectedShard);
                }
                if (isEnableDeDupe()) {
                    storeDataForDeDupe(magazineIdentifier, data);
                }
                return success;
            }
            return true;
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_LOADING_DATA, magazineIdentifier, lock);
        } finally {
            lockManager.release(lock);
        }
    }

    @Override
    public boolean reload(final String magazineIdentifier, final T data) {
        validateDataType(data);
        final Lock lock = lockManager
                .getLockInstance(String.join(Constants.KEY_DELIMITER, magazineIdentifier, data.toString()), LockLevel.DC);
        try {
            // Acquire lock if deDupe is enabled.
            if (isEnableDeDupe()) {
                lockManager.acquire(lock);
            }

            final Integer selectedShard = selectShard();
            final long loadPointer = incrementAndGetLoadPointer(magazineIdentifier, selectedShard);
            final String key = createKey(magazineIdentifier, selectedShard, String.valueOf(loadPointer));
            final boolean success = loadData(key, data);
            if (success) {
                decrementFireCounter(magazineIdentifier, selectedShard);
            }
            return success;
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_LOADING_DATA, magazineIdentifier, lock);
        } finally {
            lockManager.release(lock);
        }
    }

    @Override
    public MagazineData<T> fire(final String magazineIdentifier) {
        return fireWithRetry(magazineIdentifier);
    }

    @Override
    public Map<String, MetaData> getMetaData(final String magazineIdentifier) {
        try {
            final Record[] counterRecords = (Record[]) retryerFactory.getRetryer()
                    .call(() -> {
                        Key[] keys = createMetaKeys(magazineIdentifier, Constants.COUNTERS);
                        return aerospikeClient.get(aerospikeClient.getBatchPolicyDefault(), keys);
                    });

            final Record[] pointerRecords = (Record[]) retryerFactory.getRetryer()
                    .call(() -> {
                        Key[] keys = createMetaKeys(magazineIdentifier, Constants.POINTERS);
                        return aerospikeClient.get(aerospikeClient.getBatchPolicyDefault(), keys);
                    });

            return IntStream.range(0, getShards())
                    .boxed()
                    .collect(Collectors.toMap(
                            i -> String.join(Constants.KEY_DELIMITER, Constants.SHARD_PREFIX, String.valueOf(i)),
                            i -> MetaData.builder()
                                    .fireCounter(counterRecords[i] != null
                                            ? counterRecords[i].getLong(Constants.FIRE_COUNTER)
                                            : 0L)
                                    .loadCounter(counterRecords[i] != null
                                            ? counterRecords[i].getLong(Constants.LOAD_COUNTER)
                                            : 0L)
                                    .firePointer(pointerRecords[i] != null
                                            ? pointerRecords[i].getLong(Constants.FIRE_POINTER)
                                            : 0L)
                                    .loadPointer(pointerRecords[i] != null
                                            ? pointerRecords[i].getLong(Constants.LOAD_POINTER)
                                            : 0L)
                                    .build()));
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_GETTING_META_DATA, magazineIdentifier, null);
        }
    }

    @Override
    public void delete(final MagazineData<T> magazineData) {
        try {
            retryerFactory.getRetryer().call(() -> {
                aerospikeClient.delete(
                        aerospikeClient.getWritePolicyDefault(),
                        new Key(namespace, dataSetName, magazineData.createAerospikeKey()));
                return true;
            });
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_DELETING_DATA, magazineData.getMagazineIdentifier(), null);
        }
    }

    @Override
    public Set<MagazineData<T>> peek(final String magazineIdentifier, final Map<Integer, Set<Long>> shardPointersMap) {
        try {
            // Builds keys
            final List<Pair<Key, MagazineData.MagazineDataBuilder<T>>> keyAndMagazineDataBuilderList =
                    buildKeyAndMagazineDataList(magazineIdentifier, shardPointersMap);

            // Fetch records
            final Record[] records = (Record[]) retryerFactory.getRetryer()
                    .call(() -> aerospikeClient.get(
                            aerospikeClient.getBatchPolicyDefault(),
                            keyAndMagazineDataBuilderList.stream()
                                    .map(Pair::getKey)
                                    .collect(Collectors.toList())
                                    .toArray(Key[]::new))
                    );

            return IntStream.range(0, keyAndMagazineDataBuilderList.size())
                    .boxed()
                    .filter(i -> Objects.nonNull(records[i]))
                    .map(i -> keyAndMagazineDataBuilderList.get(i)
                            .getRight()
                            .data(clazz.cast(records[i].getValue(Constants.DATA)))
                            .build())
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_PEEKING_DATA, magazineIdentifier, null);
        }
    }

    private boolean loadData(final String key, final T data) throws ExecutionException, RetryException {
        return (Boolean) retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    writePolicy.expiration = getRecordTtl();
                    writePolicy.sendKey = true;
                    aerospikeClient.put(writePolicy,
                            new Key(namespace, dataSetName, key),
                            new Bin(Constants.DATA, data),
                            new Bin(Constants.MODIFIED_AT, System.currentTimeMillis()));
                    return true;
                });
    }

    // Retry until the record is non null or there is nothing to fire
    private MagazineData<T> fireWithRetry(final String magazineIdentifier) {
        try {
            return (MagazineData<T>) retryerFactory.getFireRetryer()
                    .call(() -> {
                        final Integer selectedShard = getRandomShardForFire(magazineIdentifier);

                        final Record pointerRecord = (Record) retryerFactory.getRetryer()
                                .call(() -> {
                                    final String key = createKey(magazineIdentifier, selectedShard, Constants.POINTERS);
                                    return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(),
                                            new Key(namespace, metaSetName, key));
                                });
                        final long currentLoadPointer = pointerRecord.getLong(Constants.LOAD_POINTER);
                        final long currentFirePointer = pointerRecord.getLong(Constants.FIRE_POINTER);

                        MagazineData magazineData = null;
                        if (currentFirePointer < currentLoadPointer) {
                            final long firePointer = incrementAndGetFirePointer(magazineIdentifier, selectedShard)
                                    .getLong(Constants.FIRE_POINTER);
                            final Record dataRecord = fireData(magazineIdentifier, selectedShard, firePointer);
                            if (Objects.nonNull(dataRecord)) {
                                magazineData = MagazineData.builder()
                                        .firePointer(firePointer)
                                        .shard(selectedShard)
                                        .magazineIdentifier(magazineIdentifier)
                                        .data(clazz.cast(dataRecord.getValue(Constants.DATA)))
                                        .build();
                                incrementFireCounter(magazineIdentifier, selectedShard);
                            }
                        }
                        return magazineData;
                    });
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_FIRING_DATA, magazineIdentifier, null);
        }
    }

    private Record fireData(final String magazineIdentifier, final Integer shard, final long firePointer)
            throws ExecutionException, RetryException {
        return (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final String key = createKey(magazineIdentifier, shard, String.valueOf(firePointer));
                    return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(),
                            new Key(namespace, dataSetName, key));
                });
    }

    private long incrementAndGetLoadPointer(final String magazineIdentifier, final Integer selectedShard)
            throws ExecutionException,
            RetryException {
        final Record record = (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
                    writePolicy.expiration = getMetaDataTtl();

                    final String key = createKey(magazineIdentifier, selectedShard, Constants.POINTERS);
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.LOAD_POINTER, 1L)),
                            Operation.get(Constants.LOAD_POINTER));
                });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessage.ERROR_READING_POINTERS, magazineIdentifier))
                    .build();
        }
        return record.getLong(Constants.LOAD_POINTER);
    }

    private Record incrementAndGetFirePointer(final String magazineIdentifier, final Integer selectedShard)
            throws ExecutionException,
            RetryException {
        return (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = aerospikeClient.getWritePolicyDefault();
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
                    writePolicy.expiration = getMetaDataTtl();

                    final String key = createKey(magazineIdentifier, selectedShard, Constants.POINTERS);
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.FIRE_POINTER, 1)),
                            Operation.get(Constants.FIRE_POINTER));
                });
    }

    private void incrementLoadCounter(final String magazineIdentifier, final Integer selectedShard)
            throws ExecutionException, RetryException {
        final Record record = (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
                    writePolicy.expiration = getMetaDataTtl();

                    final String key = createKey(magazineIdentifier, selectedShard, Constants.COUNTERS);
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.LOAD_COUNTER, 1L)),
                            Operation.get(Constants.LOAD_COUNTER));
                });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessage.ERROR_READING_COUNTERS, magazineIdentifier))
                    .build();
        }
    }

    private void incrementFireCounter(final String magazineIdentifier, final Integer shard) throws ExecutionException,
            RetryException {
        final Record record = (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
                    writePolicy.expiration = getMetaDataTtl();

                    final String key = createKey(magazineIdentifier, shard, Constants.COUNTERS);
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.FIRE_COUNTER, 1L)),
                            Operation.get(Constants.FIRE_COUNTER));
                });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessage.ERROR_READING_COUNTERS, magazineIdentifier))
                    .build();
        }
    }

    private void decrementFireCounter(final String magazineIdentifier, final Integer selectedShard)
            throws ExecutionException,
            RetryException {
        final Record record = (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
                    writePolicy.expiration = getMetaDataTtl();

                    final String key = createKey(magazineIdentifier, selectedShard, Constants.COUNTERS);
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.FIRE_COUNTER, -1L)),
                            Operation.get(Constants.FIRE_COUNTER));
                });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessage.ERROR_READING_COUNTERS, magazineIdentifier))
                    .build();
        }
    }

    // Select any random shard from active shards to fire data
    private Integer getRandomShardForFire(final String magazineIdentifier) throws InterruptedException,
            ExecutionException {
        final List<String> activeShards = getActiveShards(magazineIdentifier);
        return getShards() > 1
                ? Integer.parseInt(activeShards.get(random.nextInt(activeShards.size()))
                .split(Constants.KEY_DELIMITER)[1])
                : null;
    }

    // Get active shards from cache and throw exception if there is nothing to fire in any shard
    private List<String> getActiveShards(final String magazineIdentifier) throws InterruptedException,
            ExecutionException {
        final List<String> activeShards = activeShardsCache.get(magazineIdentifier)
                .get();
        if (activeShards.isEmpty()) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.NOTHING_TO_FIRE)
                    .message(String.format(ErrorMessage.NO_DATA_TO_FIRE, magazineIdentifier))
                    .build();
        }
        return activeShards;
    }

    // Key contains shard number if shard is non null
    private String createKey(final String magazineIdentifier, final Integer shard, final String suffix) {
        return shard != null
                ? String.join(Constants.KEY_DELIMITER,
                magazineIdentifier,
                Constants.SHARD_PREFIX,
                String.valueOf(shard),
                suffix)
                : String.join(Constants.KEY_DELIMITER, magazineIdentifier, suffix);
    }

    // Generate keys for batch read in case of sharded magazine
    private Key[] createMetaKeys(final String magazineIdentifier, final String suffix) {
        return getShards() > 1
                ? IntStream.range(0, getShards())
                .boxed()
                .map(shard -> new Key(namespace,
                        metaSetName,
                        String.join(Constants.KEY_DELIMITER,
                                magazineIdentifier,
                                Constants.SHARD_PREFIX,
                                String.valueOf(shard),
                                suffix)))
                .toArray(Key[]::new)
                : new Key[]{new Key(namespace,
                        metaSetName,
                        String.join(Constants.KEY_DELIMITER, magazineIdentifier, suffix))};
    }

    // return null if magazine is unsharded or have 1 shard, else select any random shard
    private Integer selectShard() {
        return getShards() > 1
                ? random.nextInt(getShards())
                : null;
    }

    // return false if data already exists in the magazine
    private boolean alreadyExists(final String magazineIdentifier, final T data)
            throws ExecutionException, RetryException {
        return (Boolean) retryerFactory.getRetryer()
                .call(() -> aerospikeClient.exists(aerospikeClient.getReadPolicyDefault(),
                        buildDeDuperKey(magazineIdentifier, data)));
    }

    private void storeDataForDeDupe(final String magazineIdentifier, final T data)
            throws ExecutionException, RetryException {
        retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    writePolicy.expiration = getRecordTtl();
                    writePolicy.sendKey = false;
                    aerospikeClient.put(writePolicy,
                            buildDeDuperKey(magazineIdentifier, data),
                            new Bin(Constants.MODIFIED_AT, System.currentTimeMillis()));
                    return true;
                });
    }

    private Key buildDeDuperKey(String magazineIdentifier,
            T data) {
        return new Key(
                namespace,
                DEDUPER_SET_FORMAT.formatted(getClientId()),
                magazineIdentifier + data
        );
    }

    private List<Pair<Key, MagazineData.MagazineDataBuilder<T>>> buildKeyAndMagazineDataList(
            final String magazineIdentifier,
            final Map<Integer, Set<Long>> shardPointersMap) {
        return shardPointersMap.entrySet()
                .stream()
                .flatMap(shardPointersEntry ->
                        shardPointersEntry.getValue()
                                .stream()
                                .map(pointer -> Pair.of(
                                        createKey(magazineIdentifier, shardPointersEntry.getKey(), String.valueOf(pointer)),
                                        MagazineData.<T>builder()
                                                .firePointer(pointer)
                                                .shard(shardPointersEntry.getKey())
                                                .magazineIdentifier(magazineIdentifier)
                                ))
                                .collect(Collectors.toSet())
                                .stream()
                )
                .map(keyAndMagazineDataPair -> Pair.of(
                        new Key(namespace, dataSetName, keyAndMagazineDataPair.getLeft()),
                        keyAndMagazineDataPair.getRight()
                ))
                .collect(Collectors.toList());
    }

    private AsyncLoadingCache<String, List<String>> initializeCache() {
        return Caffeine.newBuilder()
                .maximumSize(Constants.DEFAULT_MAX_ELEMENTS)
                .refreshAfterWrite(Constants.DEFAULT_REFRESH, TimeUnit.SECONDS)
                .buildAsync(key -> getMetaData(key).entrySet()
                        .stream()
                        .filter(entry -> {
                            final MetaData metaData = entry.getValue();
                            return ((metaData.getLoadCounter() > metaData.getFireCounter())
                                    && (metaData.getLoadPointer() > metaData.getFirePointer()));
                        })
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList()));
    }

    private void validateDataType(T data) {
        if (!data.getClass().isAssignableFrom(clazz)) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.DATA_TYPE_MISMATCH)
                    .message("Mismatch in data type of magazine and requested data.")
                    .build();
        }
    }

    private MagazineException handleException(final Exception exception,
                                              final String errorMessage,
                                              final String magazineIdentifier,
                                              final Lock lock) {
        if (exception instanceof MagazineException || exception.getCause() instanceof MagazineException) {
            return MagazineException.propagate(exception);
        } else if (exception instanceof DLSException) {
            if (com.phonepe.growth.dlm.exception.ErrorCode.LOCK_UNAVAILABLE
                    .equals(((DLSException) exception).getErrorCode())) {
                return MagazineException.builder()
                        .errorCode(ErrorCode.ACTION_DENIED_PARALLEL_ATTEMPT)
                        .message(String.format("Error acquiring lock - %s", (lock != null)
                                ? lock.getLockId()
                                : null))
                        .cause(exception)
                        .build();

            }
        } else if (exception instanceof RetryException) {
            return MagazineException.builder()
                    .cause(exception)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format(errorMessage, magazineIdentifier))
                    .build();
        } else if (exception instanceof ExecutionException) {
            return MagazineException.builder()
                    .cause(exception)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format(errorMessage, magazineIdentifier))
                    .build();
        }
        return MagazineException.propagate(exception);
    }
}
