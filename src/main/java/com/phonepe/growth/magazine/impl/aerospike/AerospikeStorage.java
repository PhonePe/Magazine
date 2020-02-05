package com.phonepe.growth.magazine.impl.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.rholder.retry.RetryException;
import com.phonepe.growth.magazine.common.Constants;
import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.StorageType;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import com.phonepe.growth.magazine.util.ErrorMessages;
import com.phonepe.growth.magazine.util.LockUtils;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Data
@EqualsAndHashCode(callSuper = true)
@SuppressWarnings("unchecked")
public class AerospikeStorage<T> extends BaseMagazineStorage<T> {

    private final AerospikeClient aerospikeClient;
    private final String namespace;
    private final String dataSetName;
    private final String metaSetName;
    private final AerospikeRetryer retryer;
    private final Class<T> clazz;
    private final Random random;
    private AsyncLoadingCache<String, List<String>> activeShardCache;

    @Builder
    public AerospikeStorage(AerospikeClient aerospikeClient, String namespace, String dataSetName, String metaSetName,
                            Class<T> klass, boolean enableDeDupe, int recordTtl, int shards) {
        super(StorageType.AEROSPIKE, recordTtl, enableDeDupe, shards);
        this.aerospikeClient = aerospikeClient;
        this.namespace = namespace;
        this.dataSetName = dataSetName;
        this.metaSetName = metaSetName;
        this.random = new Random();
        this.retryer = new AerospikeRetryer();
        this.clazz = klass;
        this.activeShardCache = initializeCache();
        if (enableDeDupe) {
            createIndex(dataSetName, Constants.DATA);
        }
    }

    @Override
    public boolean load(String magazineIdentifier, T data) {
        boolean lockAcquired = false;
        final String lockId = String.join(Constants.KEY_DELIMITER, magazineIdentifier, data.toString());
        try {
            //Acquire lock if deDupe is enabled.
            if (isEnableDeDupe()) {
                lockAcquired = LockUtils.acquireLock(lockId); // Exception is thrown if acquiring lock fails.
            }
            if (!isEnableDeDupe() || (isEnableDeDupe() && !alreadyExists(String.valueOf(data)))) {
                Integer selectedShard = selectShard();
                long loadPointer = incrementAndGetLoadPointer(magazineIdentifier, selectedShard);
                final String key = createKey(magazineIdentifier, selectedShard, String.valueOf(loadPointer));
                boolean success = loadData(key, data);
                if (success) {
                    incrementLoadCounter(magazineIdentifier, selectedShard);
                }
                return success;
            }
            return true;
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format(ErrorMessages.ERROR_LOADING_DATA, magazineIdentifier))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format(ErrorMessages.ERROR_LOADING_DATA, magazineIdentifier))
                    .build();
        } finally {
            if (lockAcquired) {
                LockUtils.releaseLock(lockId);
            }
        }
    }

    @Override
    public boolean reload(String magazineIdentifier, T data) {
        boolean lockAcquired = false;
        final String lockId = String.join(Constants.KEY_DELIMITER, magazineIdentifier, data.toString());
        try {
            //Acquire lock if deDupe is enabled.
            if (isEnableDeDupe()) {
                lockAcquired = LockUtils.acquireLock(lockId); // Exception is thrown if acquiring lock fails.
            }

            Integer selectedShard = selectShard();
            long loadPointer = incrementAndGetLoadPointer(magazineIdentifier, selectedShard);
            final String key = createKey(magazineIdentifier, selectedShard, String.valueOf(loadPointer));
            boolean success = loadData(key, data);
            if (success) {
                decrementFireCounter(magazineIdentifier, selectedShard);
            }
            return success;
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format(ErrorMessages.ERROR_LOADING_DATA, magazineIdentifier))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format(ErrorMessages.ERROR_LOADING_DATA, magazineIdentifier))
                    .build();
        } finally {
            if (lockAcquired) {
                LockUtils.releaseLock(lockId);
            }
        }
    }

    @Override
    public Optional<T> fire(String magazineIdentifier) {
        return Optional.of(clazz.cast(fireWithRetry(magazineIdentifier).getValue(Constants.DATA)));
    }

    @Override
    public Map<String, MetaData> getMetaData(String magazineIdentifier) {
        try {
            Record[] counterRecords = (Record[]) retryer.getRetryer().call(() -> {
                Key[] keys = createMetaKeys(magazineIdentifier, Constants.COUNTERS);
                return aerospikeClient.get(aerospikeClient.getBatchPolicyDefault(), keys);
            });

            Record[] pointerRecords = (Record[]) retryer.getRetryer().call(() -> {
                Key[] keys = createMetaKeys(magazineIdentifier, Constants.POINTERS);
                return aerospikeClient.get(aerospikeClient.getBatchPolicyDefault(), keys);
            });

            return IntStream.range(0, getShards()).boxed()
                    .collect(Collectors.toMap(i -> Constants.SHARD_PREFIX + i, i -> MetaData.builder()
                            .fireCounter(counterRecords[i] != null ? counterRecords[i].getLong(Constants.FIRE_COUNTER) : 0L)
                            .loadCounter(counterRecords[i] != null ? counterRecords[i].getLong(Constants.LOAD_COUNTER) : 0L)
                            .firePointer(pointerRecords[i] != null ? pointerRecords[i].getLong(Constants.FIRE_POINTER) : 0L)
                            .loadPointer(pointerRecords[i] != null ? pointerRecords[i].getLong(Constants.LOAD_POINTER) : 0L)
                            .build()));
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format(ErrorMessages.ERROR_GETTING_META_DATA, magazineIdentifier))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format(ErrorMessages.ERROR_GETTING_META_DATA, magazineIdentifier))
                    .build();
        }
    }

    private boolean loadData(final String key, T data) throws ExecutionException, RetryException {
        return (Boolean) retryer.getRetryer().call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            writePolicy.expiration = getRecordTtl();
            aerospikeClient.put(writePolicy,
                    new Key(namespace, dataSetName, key),
                    new Bin(Constants.DATA, data),
                    new Bin(Constants.MODIFIED_AT, System.currentTimeMillis()));
            return true;
        });
    }

    //Retry until the record is non null or there is nothing to fire
    private Record fireWithRetry(String magazineIdentifier) {
        try {
            return (Record) retryer.getFireRetryer().call(() -> {
                Integer selectedShard = getRandomShardForFire(magazineIdentifier);
                Record record = incrementAndGetFirePointer(magazineIdentifier, selectedShard);
                Record firedData = fireData(magazineIdentifier, selectedShard, record);
                incrementFireCounter(magazineIdentifier, selectedShard);
                return firedData;
            });
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format(ErrorMessages.ERROR_FIRING_DATA, magazineIdentifier))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.propagate(e);
        }
    }

    private Record fireData(String magazineIdentifier, Integer shard, Record record) throws ExecutionException, RetryException {
        return (Record) retryer.getRetryer().call(() -> {
            String key = createKey(magazineIdentifier, shard, String.valueOf(record.getLong(Constants.FIRE_POINTER)));
            return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, dataSetName, key));
        });
    }

    private long incrementAndGetLoadPointer(String magazineIdentifier, Integer selectedShard) throws ExecutionException, RetryException {
        Record record = (Record) retryer.getRetryer().call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            String key = createKey(magazineIdentifier, selectedShard, Constants.POINTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.LOAD_POINTER, 1L)),
                    Operation.get(Constants.LOAD_POINTER));
        });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessages.ERROR_READING_POINTERS, magazineIdentifier))
                    .build();
        }
        return record.getLong(Constants.LOAD_POINTER);
    }

    private Record incrementAndGetFirePointer(String magazineIdentifier, Integer selectedShard) throws ExecutionException, RetryException {
        return (Record) retryer.getRetryer().call(() -> {
            final WritePolicy writePolicy = aerospikeClient.getWritePolicyDefault();
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;

            final String key = createKey(magazineIdentifier, selectedShard, Constants.POINTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.FIRE_POINTER, 1)),
                    Operation.get(Constants.FIRE_POINTER));
        });
    }

    private void incrementLoadCounter(String magazineIdentifier, Integer selectedShard) throws ExecutionException, RetryException {
        Record record = (Record) retryer.getRetryer().call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            String key = createKey(magazineIdentifier, selectedShard, Constants.COUNTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.LOAD_COUNTER, 1L)),
                    Operation.get(Constants.LOAD_COUNTER));
        });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessages.ERROR_READING_COUNTERS, magazineIdentifier))
                    .build();
        }
    }

    private void incrementFireCounter(String magazineIdentifier, Integer shard) throws ExecutionException, RetryException {
        Record record = (Record) retryer.getRetryer().call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            String key = createKey(magazineIdentifier, shard, Constants.COUNTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.FIRE_COUNTER, 1L)),
                    Operation.get(Constants.FIRE_COUNTER));
        });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessages.ERROR_READING_COUNTERS, magazineIdentifier))
                    .build();
        }
    }

    private void decrementFireCounter(String magazineIdentifier, Integer selectedShard) throws ExecutionException, RetryException {
        Record record = (Record) retryer.getRetryer().call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            String key = createKey(magazineIdentifier, selectedShard, Constants.COUNTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.FIRE_COUNTER, -1L)),
                    Operation.get(Constants.FIRE_COUNTER));
        });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format(ErrorMessages.ERROR_READING_COUNTERS, magazineIdentifier))
                    .build();
        }
    }

    //Select any random shard from active shards to fire data
    private Integer getRandomShardForFire(String magazineIdentifier) throws InterruptedException, ExecutionException {
        List<String> activeShards = getActiveShards(magazineIdentifier);
        return getShards() > 1 ? Integer.parseInt(activeShards.get(random.nextInt(activeShards.size())).split(Constants.KEY_DELIMITER)[1]) : null;
    }

    //Get active shards from cache and throw exception if there is nothing to fire in any shard
    private List<String> getActiveShards(String magazineIdentifier) throws InterruptedException, ExecutionException {
        List<String> activeShards = activeShardCache.get(magazineIdentifier).get();
        if (activeShards.isEmpty()) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.NOTHING_TO_FIRE)
                    .message(String.format(ErrorMessages.NO_DATA_TO_FIRE, magazineIdentifier))
                    .build();
        }
        return activeShards;
    }

    //Key contains shard number if shard is non null
    private String createKey(String magazineIdentifier, Integer shard, String suffix) {
        return shard != null ? String.join(Constants.KEY_DELIMITER, magazineIdentifier, String.valueOf(shard), suffix) : String.join(Constants.KEY_DELIMITER, magazineIdentifier, suffix);
    }

    //Generate keys for batch read in case of sharded magazine
    private Key[] createMetaKeys(String magazineIdentifier, String suffix) {
        return getShards() > 1 ? IntStream.range(0, getShards()).boxed()
                .map(shard -> new Key(namespace, metaSetName, String.join(Constants.KEY_DELIMITER, magazineIdentifier, String.valueOf(shard), suffix)))
                .toArray(Key[]::new)
                : new Key[]{new Key(namespace, metaSetName, String.join(Constants.KEY_DELIMITER, magazineIdentifier, suffix))};
    }

    //return null if magazine is unsharded or have 1 shard, else select any random shard
    private Integer selectShard() {
        return getShards() > 1 ? random.nextInt(getShards()) : null;
    }

    //return false if data already exists in the magazine
    private boolean alreadyExists(String data) throws ExecutionException, RetryException {
        return (Boolean) retryer.getRetryer().call(() -> {
            Statement statement = new Statement();
            statement.setNamespace(namespace);
            statement.setSetName(dataSetName);
            statement.setIndexName(Constants.DATA);
            statement.setFilter(Filter.equal(Constants.DATA, data));
            RecordSet rs = aerospikeClient.query(null, statement);
            return rs.next();
        });
    }

    private AsyncLoadingCache<String, List<String>> initializeCache() {
        return Caffeine.newBuilder()
                .maximumSize(Constants.DEFAULT_MAX_ELEMENTS)
                .refreshAfterWrite(Constants.DEFAULT_REFRESH, TimeUnit.SECONDS)
                .buildAsync(key -> getMetaData(key).entrySet().stream()
                        .filter(entry -> {
                            MetaData metaData = entry.getValue();
                            return ((metaData.getLoadCounter() > metaData.getFireCounter()) && (metaData.getLoadPointer() > metaData.getFirePointer()));
                        }).map(Map.Entry::getKey).collect(Collectors.toList()));
    }

    private void createIndex(String setName, String bin) {
        try {
            aerospikeClient.createIndex(null, namespace, setName, setName, bin, IndexType.STRING).waitTillComplete();
        } catch (AerospikeException e) {
            if (e.getResultCode() == 200) {
                return;
            }
            throw e;
        }
    }
}
