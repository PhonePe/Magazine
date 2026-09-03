/**
 * Copyright (c) 2025 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.phonepe.magazine.impl.aerospike;

import com.aerospike.client.Bin;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.rholder.retry.RetryException;
import com.phonepe.dlm.DistributedLockManager;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.lock.Lock;
import com.phonepe.dlm.lock.base.LockBase;
import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.dlm.lock.mode.LockMode;
import com.phonepe.dlm.lock.storage.aerospike.AerospikeStore;
import com.phonepe.magazine.common.Constants;
import com.phonepe.magazine.common.MagazineData;
import com.phonepe.magazine.common.MetaData;
import com.phonepe.magazine.MagazineContext;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.core.StorageType;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.scope.MagazineScope;
import com.phonepe.magazine.util.CommonUtils;
import com.phonepe.magazine.util.ErrorMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.security.SecureRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@EqualsAndHashCode(callSuper = true)
public class AerospikeStorage<T> extends BaseMagazineStorage<T> {

    private static final String DEDUPER_SET_FORMAT = "%s_deduper";
    private final IAerospikeClient aerospikeClient;
    private final String namespace;
    private final String dataSetName;
    private final String metaSetName;
    private final AerospikeRetryerFactory<Object> retryerFactory;
    private final Class<T> clazz;
    private final Random random = new SecureRandom();
    @Getter(AccessLevel.NONE)
    private final String[] shardIds;
    private final AsyncLoadingCache<MagazineContext, List<String>> activeShardsCache;
    private final DistributedLockManager lockManager;
    private final LockLevel lockLevel;

    @Builder
    public AerospikeStorage(
            final IAerospikeClient aerospikeClient,
            final AerospikeStorageConfig storageConfig,
            final boolean enableDeDupe,
            final String farmId,
            final Class<T> clazz,
            final String clientId,
            final MagazineScope scope) {
        super(StorageType.AEROSPIKE, validateStorageConfig(storageConfig).getRecordTtl(),
                storageConfig.getMetaDataTtl(), farmId, enableDeDupe, storageConfig.getShards(), clientId, scope);
        validateStorage(aerospikeClient, clazz, enableDeDupe);
        this.clazz = clazz;
        this.aerospikeClient = aerospikeClient;
        this.namespace = storageConfig.getNamespace();
        this.dataSetName = CommonUtils.resolveSetName(storageConfig.getDataSetName(), farmId, scope);
        this.metaSetName = CommonUtils.resolveSetName(storageConfig.getMetaSetName(), farmId, scope);
        this.retryerFactory = new AerospikeRetryerFactory<>();
        this.shardIds = createShardIds();
        this.activeShardsCache = initializeCache();
        if (enableDeDupe) {
            this.lockManager = new DistributedLockManager(Constants.DLM_CLIENT_ID, farmId,
                    LockBase.builder()
                            .mode(LockMode.EXCLUSIVE)
                            .lockStore(AerospikeStore.builder()
                                    .aerospikeClient(aerospikeClient)
                                    .namespace(namespace)
                                    .setSuffix(Constants.MAGAZINE_DISTRIBUTED_LOCK_SET_NAME_SUFFIX)
                                    .build())
                            .build());
            this.lockLevel = CommonUtils.resolveLockLevel(scope);
            lockManager.initialize();
        } else {
            this.lockManager = null;
            this.lockLevel = null;
        }
    }

    @Override
    public boolean load(final MagazineContext context,
            final T data) {
        final String magazineIdentifier = context.getMagazineIdentifier();
        validateDataType(data);
        Lock lock = null;
        boolean lockAcquired = false;
        try {
            if (isEnableDeDupe()) {
                lock = lockManager.getLockInstance(
                        String.join(Constants.KEY_DELIMITER, magazineIdentifier, data.toString()), lockLevel);
                lockManager.tryAcquireLock(lock); // Exception is thrown if acquiring lock fails.
                lockAcquired = true;
            }
            if (!isEnableDeDupe() || !alreadyExists(magazineIdentifier, data)) {
                final Integer selectedShard = selectShard();
                final long loadPointer = incrementAndGetLoadPointer(context, selectedShard);
                final String key = createKey(magazineIdentifier, selectedShard, String.valueOf(loadPointer));
                final boolean success = loadData(key, data);
                if (success) {
                    incrementLoadCounter(context, selectedShard);
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
            releaseLock(lock, lockAcquired);
        }
    }

    @Override
    public boolean reload(final MagazineContext context,
            final T data) {
        final String magazineIdentifier = context.getMagazineIdentifier();
        validateDataType(data);
        Lock lock = null;
        boolean lockAcquired = false;
        try {
            if (isEnableDeDupe()) {
                lock = lockManager.getLockInstance(
                        String.join(Constants.KEY_DELIMITER, magazineIdentifier, data.toString()), lockLevel);
                lockManager.tryAcquireLock(lock);
                lockAcquired = true;
            }

            final Integer selectedShard = selectShard();
            final long loadPointer = incrementAndGetLoadPointer(context, selectedShard);
            final String key = createKey(magazineIdentifier, selectedShard, String.valueOf(loadPointer));
            final boolean success = loadData(key, data);
            if (success) {
                decrementFireCounter(context, selectedShard);
            }
            return success;
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_LOADING_DATA, magazineIdentifier, lock);
        } finally {
            releaseLock(lock, lockAcquired);
        }
    }

    @Override
    public MagazineData<T> fire(final MagazineContext context) {
        return fireWithRetry(context);
    }

    @Override
    public Map<String, MetaData> getMetaData(final MagazineContext context) {
        final String magazineIdentifier = context.getMagazineIdentifier();
        try {
            final boolean unifiedMetadata = usesUnifiedMetadata(context);
            final Record[] pointerRecords = getMetaRecords(magazineIdentifier,
                    unifiedMetadata ? Constants.METADATA : Constants.POINTERS);
            final Record[] counterRecords = unifiedMetadata
                    ? pointerRecords
                    : getMetaRecords(magazineIdentifier, Constants.COUNTERS);

            final Map<String, MetaData> metaData = new HashMap<>(getShards());
            for (int shard = 0; shard < getShards(); shard++) {
                final Record pointerRecord = pointerRecords[shard];
                final Record counterRecord = counterRecords[shard];
                metaData.put(shardIds[shard], new MetaData(
                        Objects.nonNull(counterRecord)
                                ? counterRecord.getLong(Constants.FIRE_COUNTER)
                                : 0L,
                        Objects.nonNull(counterRecord)
                                ? counterRecord.getLong(Constants.LOAD_COUNTER)
                                : 0L,
                        Objects.nonNull(pointerRecord)
                                ? pointerRecord.getLong(Constants.FIRE_POINTER)
                                : 0L,
                        Objects.nonNull(pointerRecord)
                                ? pointerRecord.getLong(Constants.LOAD_POINTER)
                                : 0L));
            }
            return metaData;
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_GETTING_META_DATA, magazineIdentifier, null);
        }
    }

    @Override
    public void delete(final MagazineContext context, final MagazineData<T> magazineData) {
        if (!context.getMagazineIdentifier().equals(magazineData.getMagazineIdentifier())) {
            throw invalidConfiguration("Magazine data belongs to a different magazine.");
        }
        deleteData(magazineData);
    }

    private void deleteData(final MagazineData<T> magazineData) {
        try {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            retryerFactory.getRetryer()
                    .call(() -> {
                        aerospikeClient.delete(
                                writePolicy,
                                new Key(namespace, dataSetName, magazineData.createAerospikeKey()));
                        return true;
                    });
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_DELETING_DATA, magazineData.getMagazineIdentifier(), null);
        }
    }

    @Override
    public Set<MagazineData<T>> peek(final MagazineContext context,
            final Map<Integer, Set<Long>> shardPointersMap) {
        return peekData(context.getMagazineIdentifier(), shardPointersMap);
    }

    private Set<MagazineData<T>> peekData(final String magazineIdentifier,
            final Map<Integer, Set<Long>> shardPointersMap) {
        try {
            final List<PeekRequest> requests = new ArrayList<>();
            final List<BatchRead> batchReads = new ArrayList<>();
            for (Map.Entry<Integer, Set<Long>> entry : shardPointersMap.entrySet()) {
                for (long pointer : entry.getValue()) {
                    final Key key = new Key(namespace, dataSetName,
                            createKey(magazineIdentifier, entry.getKey(), String.valueOf(pointer)));
                    batchReads.add(new BatchRead(key, true));
                    requests.add(new PeekRequest(entry.getKey(), pointer));
                }
            }

            retryerFactory.getRetryer()
                    .call(() -> aerospikeClient.get(aerospikeClient.getBatchPolicyDefault(), batchReads));

            final Set<MagazineData<T>> magazineData = new HashSet<>(requests.size());
            for (int i = 0; i < requests.size(); i++) {
                final BatchRead batchRead = batchReads.get(i);
                if (batchRead.resultCode != ResultCode.OK
                        && batchRead.resultCode != ResultCode.KEY_NOT_FOUND_ERROR) {
                    throw MagazineException.builder()
                            .errorCode(ErrorCode.CONNECTION_ERROR)
                            .message(String.format(ErrorMessage.ERROR_PEEKING_DATA, magazineIdentifier))
                            .build();
                }
                final Record record = batchRead.record;
                if (Objects.nonNull(record)) {
                    final PeekRequest request = requests.get(i);
                    magazineData.add(new MagazineData<>(
                            clazz.cast(record.getValue(Constants.DATA)),
                            request.pointer(),
                            request.shard(),
                            magazineIdentifier));
                }
            }
            return magazineData;
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_PEEKING_DATA, magazineIdentifier, null);
        }
    }

    private boolean loadData(final String key,
            final T data) throws ExecutionException, RetryException {
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

    // Retry until the record is non-null, the queue is empty, or the retry limit is reached.
    @SuppressWarnings("unchecked")
    private MagazineData<T> fireWithRetry(final MagazineContext context) {
        final String magazineIdentifier = context.getMagazineIdentifier();
        try {
            return (MagazineData<T>) retryerFactory.getFireRetryer()
                    .call(() -> {
                        final Integer selectedShard = getRandomShardForFire(context);
                        final boolean unifiedMetadata = usesUnifiedMetadata(context);

                        final String metadataKey = createKey(
                                magazineIdentifier, selectedShard,
                                unifiedMetadata ? Constants.METADATA : Constants.POINTERS);
                        final Record metadataRecord = (Record) retryerFactory.getRetryer()
                                .call(() -> aerospikeClient.get(aerospikeClient.getReadPolicyDefault(),
                                            new Key(namespace, metaSetName, metadataKey)));
                        if (Objects.isNull(metadataRecord)) {
                            suppressActiveShard(context, selectedShard);
                            return null;
                        }
                        final long currentLoadPointer = metadataRecord.getLong(Constants.LOAD_POINTER);
                        final long currentFirePointer = metadataRecord.getLong(Constants.FIRE_POINTER);

                        MagazineData<T> magazineData = null;
                        if (currentFirePointer < currentLoadPointer) {
                            final long firePointer = currentFirePointer + 1;
                            final Record dataRecord = fireData(magazineIdentifier, selectedShard, firePointer);
                            if (claimFirePointer(metadataKey, currentFirePointer,
                                    Objects.nonNull(dataRecord), unifiedMetadata)) {
                                if (Objects.isNull(dataRecord)) {
                                    return null;
                                }
                                magazineData = MagazineData.<T>builder()
                                        .firePointer(firePointer)
                                        .shard(selectedShard)
                                        .magazineIdentifier(magazineIdentifier)
                                        .data(clazz.cast(dataRecord.getValue(Constants.DATA)))
                                        .build();
                                if (!unifiedMetadata) {
                                    incrementFireCounter(context, selectedShard);
                                }
                            }
                        } else {
                            suppressActiveShard(context, selectedShard);
                        }
                        return magazineData;
                    });
        } catch (Exception e) {
            if (isInterrupted(e)) {
                Thread.currentThread().interrupt();
                throw MagazineException.builder()
                        .cause(e)
                        .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                        .message(String.format(ErrorMessage.ERROR_FIRING_DATA, magazineIdentifier))
                        .build();
            }
            throw handleException(e, ErrorMessage.ERROR_FIRING_DATA, magazineIdentifier, null);
        }
    }

    private Record fireData(final String magazineIdentifier,
            final Integer shard,
            final long firePointer)
            throws ExecutionException, RetryException {
        return (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final String key = createKey(magazineIdentifier, shard, String.valueOf(firePointer));
                    return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(),
                            new Key(namespace, dataSetName, key));
                });
    }

    private boolean claimFirePointer(final String metadataKey,
            final long expectedFirePointer,
            final boolean incrementCounter,
            final boolean unifiedMetadata) {
        final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        writePolicy.maxRetries = 0;
        writePolicy.failOnFilteredOut = true;
        writePolicy.filterExp = Exp.build(expectedFirePointer == 0L
                ? Exp.or(
                        Exp.not(Exp.binExists(Constants.FIRE_POINTER)),
                        Exp.eq(Exp.intBin(Constants.FIRE_POINTER), Exp.val(0L)))
                : Exp.eq(Exp.intBin(Constants.FIRE_POINTER), Exp.val(expectedFirePointer)));
        writePolicy.expiration = getMetaDataTtl();

        try {
            if (incrementCounter && unifiedMetadata) {
                aerospikeClient.operate(writePolicy,
                        new Key(namespace, metaSetName, metadataKey),
                        Operation.add(new Bin(Constants.FIRE_POINTER, 1L)),
                        Operation.add(new Bin(Constants.FIRE_COUNTER, 1L)));
            } else {
                aerospikeClient.operate(writePolicy,
                        new Key(namespace, metaSetName, metadataKey),
                        Operation.add(new Bin(Constants.FIRE_POINTER, 1L)));
            }
            return true;
        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.FILTERED_OUT) {
                return false;
            }
            throw e;
        }
    }

    private long incrementAndGetLoadPointer(final MagazineContext context,
            final Integer selectedShard)
            throws ExecutionException,
            RetryException {
        final String magazineIdentifier = context.getMagazineIdentifier();
        final Record magazineRecord = (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
                    writePolicy.expiration = getMetaDataTtl();

                    final String key = createKey(magazineIdentifier, selectedShard,
                            metadataSuffix(context, Constants.POINTERS));
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.LOAD_POINTER, 1L)),
                            Operation.get(Constants.LOAD_POINTER));
                });

        if (Objects.isNull(magazineRecord)) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessage.ERROR_READING_POINTERS, magazineIdentifier))
                    .build();
        }
        return magazineRecord.getLong(Constants.LOAD_POINTER);
    }

    private void incrementLoadCounter(final MagazineContext context,
            final Integer selectedShard)
            throws ExecutionException, RetryException {
        final String magazineIdentifier = context.getMagazineIdentifier();
        final Record magazineRecord = (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
                    writePolicy.expiration = getMetaDataTtl();

                    final String key = createKey(magazineIdentifier, selectedShard,
                            metadataSuffix(context, Constants.COUNTERS));
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.LOAD_COUNTER, 1L)),
                            Operation.get(Constants.LOAD_COUNTER));
                });

        if (Objects.isNull(magazineRecord)) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessage.ERROR_READING_COUNTERS, magazineIdentifier))
                    .build();
        }
    }

    private void incrementFireCounter(final MagazineContext context,
            final Integer selectedShard) throws ExecutionException, RetryException {
        final String magazineIdentifier = context.getMagazineIdentifier();
        final Record magazineRecord = (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
                    writePolicy.expiration = getMetaDataTtl();

                    final String key = createKey(magazineIdentifier, selectedShard, Constants.COUNTERS);
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.FIRE_COUNTER, 1L)),
                            Operation.get(Constants.FIRE_COUNTER));
                });

        if (Objects.isNull(magazineRecord)) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessage.ERROR_READING_COUNTERS, magazineIdentifier))
                    .build();
        }
    }

    private void decrementFireCounter(final MagazineContext context,
            final Integer selectedShard)
            throws ExecutionException,
            RetryException {
        final String magazineIdentifier = context.getMagazineIdentifier();
        final Record magazineRecord = (Record) retryerFactory.getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
                    writePolicy.expiration = getMetaDataTtl();

                    final String key = createKey(magazineIdentifier, selectedShard,
                            metadataSuffix(context, Constants.COUNTERS));
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.FIRE_COUNTER, -1L)),
                            Operation.get(Constants.FIRE_COUNTER));
                });

        if (Objects.isNull(magazineRecord)) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessage.ERROR_READING_COUNTERS, magazineIdentifier))
                    .build();
        }
    }

    // Select any random shard from active shards to fire data
    private Integer getRandomShardForFire(final MagazineContext context) throws InterruptedException,
            ExecutionException {
        final List<String> activeShards = getActiveShards(context);
        return getShards() > 1
                ? parseShard(activeShards.get(ThreadLocalRandom.current().nextInt(activeShards.size())))
                : null;
    }

    // Get active shards from cache and throw exception if there is nothing to fire in any shard
    private List<String> getActiveShards(final MagazineContext context) throws InterruptedException,
            ExecutionException {
        final List<String> activeShards = activeShardsCache.get(context)
                .get();
        if (activeShards.isEmpty()) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.NOTHING_TO_FIRE)
                    .message(String.format(ErrorMessage.NO_DATA_TO_FIRE, context.getMagazineIdentifier()))
                    .build();
        }
        return activeShards;
    }

    // Key contains shard number if shard is non null
    private String createKey(final String magazineIdentifier,
            final Integer shard,
            final String suffix) {
        return Objects.nonNull(shard)
                ? magazineIdentifier + Constants.KEY_DELIMITER + getShardId(shard)
                + Constants.KEY_DELIMITER + suffix
                : magazineIdentifier + Constants.KEY_DELIMITER + suffix;
    }

    // Generate keys for batch read in case of sharded magazine
    private Key[] createMetaKeys(final String magazineIdentifier,
            final String suffix) {
        if (getShards() == 1) {
            return new Key[]{new Key(namespace,
                    metaSetName,
                    magazineIdentifier + Constants.KEY_DELIMITER + suffix)};
        }

        final Key[] keys = new Key[getShards()];
        for (int shard = 0; shard < getShards(); shard++) {
            keys[shard] = new Key(namespace,
                    metaSetName,
                    magazineIdentifier + Constants.KEY_DELIMITER + shardIds[shard]
                            + Constants.KEY_DELIMITER + suffix);
        }
        return keys;
    }

    private Record[] getMetaRecords(final String magazineIdentifier,
            final String suffix)
            throws ExecutionException, RetryException {
        return (Record[]) retryerFactory.getRetryer()
                .call(() -> aerospikeClient.get(
                        aerospikeClient.getBatchPolicyDefault(),
                        createMetaKeys(magazineIdentifier, suffix)));
    }

    private List<String> getActiveShardsFromMetadata(final MagazineContext context) {
        final String magazineIdentifier = context.getMagazineIdentifier();
        try {
            final boolean unifiedMetadata = usesUnifiedMetadata(context);
            final Record[] pointerRecords = getMetaRecords(magazineIdentifier,
                    unifiedMetadata ? Constants.METADATA : Constants.POINTERS);
            final Record[] counterRecords = unifiedMetadata
                    ? pointerRecords
                    : getMetaRecords(magazineIdentifier, Constants.COUNTERS);
            final List<String> activeShards = new ArrayList<>(getShards());
            for (int shard = 0; shard < getShards(); shard++) {
                final Record pointerRecord = pointerRecords[shard];
                final Record counterRecord = counterRecords[shard];
                if (Objects.nonNull(pointerRecord)
                        && Objects.nonNull(counterRecord)
                        && counterRecord.getLong(Constants.LOAD_COUNTER)
                        > counterRecord.getLong(Constants.FIRE_COUNTER)
                        && pointerRecord.getLong(Constants.LOAD_POINTER)
                        > pointerRecord.getLong(Constants.FIRE_POINTER)) {
                    activeShards.add(shardIds[shard]);
                }
            }
            return List.copyOf(activeShards);
        } catch (Exception e) {
            throw handleException(e, ErrorMessage.ERROR_GETTING_META_DATA, magazineIdentifier, null);
        }
    }

    private String metadataSuffix(final MagazineContext context, final String legacySuffix) {
        return usesUnifiedMetadata(context) ? Constants.METADATA : legacySuffix;
    }

    private boolean usesUnifiedMetadata(final MagazineContext context) {
        return context.getMetadataSchemaVersion() == Constants.UNIFIED_METADATA_SCHEMA_VERSION;
    }

    // return null if magazine is unsharded or have 1 shard, else select any random shard
    private Integer selectShard() {
        return getShards() > 1
                ? ThreadLocalRandom.current().nextInt(getShards())
                : null;
    }

    // return false if data already exists in the magazine
    private boolean alreadyExists(final String magazineIdentifier,
            final T data)
            throws ExecutionException, RetryException {
        return (Boolean) retryerFactory.getRetryer()
                .call(() -> aerospikeClient.exists(aerospikeClient.getReadPolicyDefault(),
                        buildDeDuperKey(magazineIdentifier, data)));
    }

    private void storeDataForDeDupe(final String magazineIdentifier,
            final T data)
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
                CommonUtils.resolveSetName(DEDUPER_SET_FORMAT.formatted(getClientId()), getFarmId(), getScope()),
                magazineIdentifier + data
        );
    }

    private String[] createShardIds() {
        final String[] ids = new String[getShards()];
        for (int shard = 0; shard < getShards(); shard++) {
            ids[shard] = Constants.SHARD_PREFIX + Constants.KEY_DELIMITER + shard;
        }
        return ids;
    }

    private String getShardId(final int shard) {
        return shard >= 0 && shard < shardIds.length
                ? shardIds[shard]
                : Constants.SHARD_PREFIX + Constants.KEY_DELIMITER + shard;
    }

    private int parseShard(final String shardId) {
        return Integer.parseInt(shardId, Constants.SHARD_PREFIX.length() + 1, shardId.length(), 10);
    }

    private AsyncLoadingCache<MagazineContext, List<String>> initializeCache() {
        return Caffeine.newBuilder()
                .maximumSize(Constants.DEFAULT_MAX_ELEMENTS)
                .refreshAfterWrite(Constants.DEFAULT_REFRESH, TimeUnit.SECONDS)
                .buildAsync(this::getActiveShardsFromMetadata);
    }

    private record PeekRequest(Integer shard, long pointer) {
    }

    private void suppressActiveShard(final MagazineContext context, final Integer shard) {
        final String shardId = shardIds[Objects.isNull(shard) ? 0 : shard];
        activeShardsCache.synchronous().asMap().computeIfPresent(context,
                (cacheKey, activeShards) -> activeShards.stream()
                        .filter(activeShard -> !shardId.equals(activeShard))
                        .toList());
    }

    private boolean isInterrupted(final Throwable throwable) {
        Throwable cause = throwable;
        while (Objects.nonNull(cause)) {
            if (cause instanceof InterruptedException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private void validateDataType(final T data) {
        if (!clazz.isInstance(data)) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.DATA_TYPE_MISMATCH)
                    .message("Mismatch in data type of magazine and requested data.")
                    .build();
        }
    }

    private static AerospikeStorageConfig validateStorageConfig(final AerospikeStorageConfig storageConfig) {
        if (Objects.isNull(storageConfig)) {
            throw invalidConfiguration("Aerospike storage configuration is required.");
        }
        if (Objects.isNull(storageConfig.getNamespace()) || storageConfig.getNamespace().isBlank()) {
            throw invalidConfiguration("Aerospike namespace is required.");
        }
        if (Objects.isNull(storageConfig.getDataSetName()) || storageConfig.getDataSetName().isBlank()) {
            throw invalidConfiguration("Aerospike data set name is required.");
        }
        if (Objects.isNull(storageConfig.getMetaSetName()) || storageConfig.getMetaSetName().isBlank()) {
            throw invalidConfiguration("Aerospike metadata set name is required.");
        }
        return storageConfig;
    }

    private static void validateStorage(final IAerospikeClient aerospikeClient,
            final Class<?> clazz,
            final boolean enableDeDupe) {
        if (Objects.isNull(aerospikeClient)) {
            throw invalidConfiguration("Aerospike client is required.");
        }
        if (Objects.isNull(clazz)) {
            throw invalidConfiguration("Magazine data type is required.");
        }
        if (enableDeDupe && !Constants.DEDUPABLE_CLASSES.contains(clazz)) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.DATA_TYPE_MISMATCH)
                    .message("Deduplication is supported only for String, Long, and Integer data types.")
                    .build();
        }
    }

    private void releaseLock(final Lock lock, final boolean lockAcquired) {
        if (!lockAcquired) {
            return;
        }
        try {
            lockManager.releaseLock(lock);
        } catch (Exception e) {
            log.warn("Error releasing deduplication lock for lock id {}", lock.getLockId(), e);
        }
    }

    private static MagazineException invalidConfiguration(final String message) {
        return MagazineException.builder()
                .errorCode(ErrorCode.INVALID_CONFIGURATION)
                .message(message)
                .build();
    }

    private MagazineException handleException(final Exception exception,
            final String errorMessage,
            final String magazineIdentifier,
            final Lock lock) {
        if (exception instanceof MagazineException || exception.getCause() instanceof MagazineException) {
            return MagazineException.propagate(exception);
        } else if (exception instanceof DLMException dlmException) {
            if (com.phonepe.dlm.exception.ErrorCode.LOCK_UNAVAILABLE
                    .equals(dlmException.getErrorCode())) {
                return MagazineException.builder()
                        .errorCode(ErrorCode.ACTION_DENIED_PARALLEL_ATTEMPT)
                        .message(String.format("Error acquiring lock - %s", Objects.nonNull(lock)
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
