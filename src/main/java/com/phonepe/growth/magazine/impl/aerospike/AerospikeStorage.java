package com.phonepe.growth.magazine.impl.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.github.rholder.retry.*;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.phonepe.growth.magazine.common.Constants;
import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.StorageType;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import com.phonepe.growth.magazine.util.LockUtils;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Data
@EqualsAndHashCode(callSuper = true)
public class AerospikeStorage<T> extends BaseMagazineStorage<T> {

    private final AerospikeClient aerospikeClient;
    private final String namespace;
    private final String dataSetName;
    private final String metaSetName;
    private final Retryer<Boolean> writeRetryer;
    private final Retryer<Record> readRetryer;
    private final Retryer<Record> fireRetryer;
    private final Class<T> clazz;

    @Builder
    public AerospikeStorage(AerospikeClient aerospikeClient,
                            String namespace,
                            String dataSetName,
                            String metaSetName,
                            Class<T> klass,
                            boolean enableDeDupe,
                            int recordTtl) {
        super(StorageType.AEROSPIKE, recordTtl, enableDeDupe);

        this.aerospikeClient = aerospikeClient;
        this.namespace = namespace;
        this.dataSetName = dataSetName;
        this.metaSetName = metaSetName;
        writeRetryer = RetryerBuilder.<Boolean>newBuilder()
                .retryIfExceptionOfType(AerospikeException.class)
                .withStopStrategy(StopStrategies.stopAfterAttempt(Constants.MAX_RETRIES))
                .withWaitStrategy(WaitStrategies.fixedWait(Constants.DELAY_BETWEEN_RETRIES, TimeUnit.MILLISECONDS))
                .withBlockStrategy(BlockStrategies.threadSleepStrategy())
                .build();
        readRetryer = RetryerBuilder.<Record>newBuilder()
                .retryIfExceptionOfType(AerospikeException.class)
                .withStopStrategy(StopStrategies.stopAfterAttempt(Constants.MAX_RETRIES))
                .withWaitStrategy(WaitStrategies.fixedWait(Constants.DELAY_BETWEEN_RETRIES, TimeUnit.MILLISECONDS))
                .withBlockStrategy(BlockStrategies.threadSleepStrategy())
                .build();
        fireRetryer = RetryerBuilder.<Record>newBuilder()
                .retryIfExceptionOfType(AerospikeException.class)
                .retryIfResult(Predicates.isNull())
                .withStopStrategy(StopStrategies.neverStop())
                .withWaitStrategy(WaitStrategies.fixedWait(Constants.DELAY_BETWEEN_RETRIES, TimeUnit.MILLISECONDS))
                .withBlockStrategy(BlockStrategies.threadSleepStrategy())
                .build();
        clazz = klass;

        if (enableDeDupe){
            createIndex(dataSetName, Constants.DATA);
        }
    }

    @Override
    public boolean load(String magazineIdentifier, T data) {
        boolean lockAcquired = false;
        final String lockId = Joiner.on("_").join(magazineIdentifier, data);
        try {
            lockAcquired = LockUtils.acquireLock(lockId); // Exception is thrown if acquiring lock fails.

            if (!isEnableDeDupe() || (isEnableDeDupe() && !alreadyExists(String.valueOf(data)))) {
                long loadPointer = incrementAndGetLoadPointer(magazineIdentifier);
                final String key = Joiner.on("_").join(magazineIdentifier, loadPointer);
                boolean success = loadData(key, data);
                if (success) {
                    incrementLoadCounter(magazineIdentifier);
                }
                return success;
            }
            return true;
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format("Error loading data [magazineIdentifier = %s]", magazineIdentifier))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error loading data [magazineIdentifier = %s]", magazineIdentifier))
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
        final String lockId = Joiner.on("_").join(magazineIdentifier, data);
        try {
            lockAcquired = LockUtils.acquireLock(lockId); // Exception is thrown if acquiring lock fails.

            long loadPointer = incrementAndGetLoadPointer(magazineIdentifier);
            final String key = Joiner.on("_").join(magazineIdentifier, loadPointer);
            boolean success = loadData(key, data);
            if (success) {
                decrementFireCounter(magazineIdentifier);
            }
            return success;
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format("Error loading data [magazineIdentifier = %s]", magazineIdentifier))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error loading data [magazineIdentifier = %s]", magazineIdentifier))
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
    public MetaData getMetaData(String magazineIdentifier) {
        try {
            Record counterRecord = readRetryer.call(() -> {
                final String key = Joiner.on("_").join(magazineIdentifier, Constants.COUNTERS);
                return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, metaSetName, key));
            });

            Record pointerRecord = readRetryer.call(() -> {
                final String key = Joiner.on("_").join(magazineIdentifier, Constants.POINTERS);
                return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, metaSetName, key));
            });

            return MetaData.builder()
                    .fireCounter(counterRecord != null ? counterRecord.getLong(Constants.FIRE_COUNTER) : 0L)
                    .loadCounter(counterRecord != null ? counterRecord.getLong(Constants.LOAD_COUNTER) : 0L)
                    .firePointer(pointerRecord != null ? pointerRecord.getLong(Constants.FIRE_POINTER) : 0L)
                    .loadPointer(pointerRecord != null ? pointerRecord.getLong(Constants.LOAD_POINTER) : 0L)
                    .build();
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format("Error getting meta data [magazineIdentifier = %s]", magazineIdentifier))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error getting meta data [magazineIdentifier = %s]", magazineIdentifier))
                    .build();
        }
    }

    private Record fireWithRetry(String magazineIdentifier) {
        try {
            return fireRetryer.call(() -> {
                Record pointerRecord = readRetryer.call(() -> {
                    final String key = Joiner.on("_").join(magazineIdentifier, Constants.POINTERS);
                    return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, metaSetName, key));
                });

                long loadPointer = pointerRecord.getLong(Constants.LOAD_POINTER);
                long firePointer = pointerRecord.getLong(Constants.FIRE_POINTER);

                if (firePointer >= loadPointer) {
                    throw MagazineException.builder()
                            .errorCode(ErrorCode.NOTHING_TO_FIRE)
                            .message(String.format("No data to fire [magazineIdentifier = %s]", magazineIdentifier))
                            .build();
                }

                Record record = readRetryer.call(() -> {
                    final WritePolicy writePolicy = aerospikeClient.getWritePolicyDefault();
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;

                    final String key = Joiner.on("_").join(magazineIdentifier, Constants.POINTERS);
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.FIRE_POINTER, 1)),
                            Operation.get(Constants.FIRE_POINTER));
                });

                //Fire data
                Record firedData = fireData(magazineIdentifier, record);
                incrementFireCounter(magazineIdentifier);
                return firedData;
            });
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format("Error firing data [magazineIdentifier = %s]", magazineIdentifier))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.propagate(e);
        }
    }

    private void incrementFireCounter(String magazineIdentifier) throws ExecutionException, RetryException {
        Record record = readRetryer.call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            String key = Joiner.on("_").join(magazineIdentifier, Constants.COUNTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.FIRE_COUNTER, 1L)),
                    Operation.get(Constants.FIRE_COUNTER));
        });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format("Error reading counters [magazineIdentifier = %s]", magazineIdentifier))
                    .build();
        }
    }

    private void decrementFireCounter(String magazineIdentifier) throws ExecutionException, RetryException {
        Record record = readRetryer.call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            String key = Joiner.on("_").join(magazineIdentifier, Constants.COUNTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.FIRE_COUNTER, -1L)),
                    Operation.get(Constants.FIRE_COUNTER));
        });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format("Error reading counters [magazineIdentifier = %s]", magazineIdentifier))
                    .build();
        }
    }

    private Record fireData(String magazineIdentifier, Record record) throws ExecutionException, RetryException {
        return readRetryer.call(() -> {
            String key = Joiner.on("_").join(magazineIdentifier, record.getLong(Constants.FIRE_POINTER));
            return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, dataSetName, key));
        });
    }

    private void incrementLoadCounter(String magazineIdentifier) throws ExecutionException, RetryException {
        Record record = readRetryer.call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            String key = Joiner.on("_").join(magazineIdentifier, Constants.COUNTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.LOAD_COUNTER, 1L)),
                    Operation.get(Constants.LOAD_COUNTER));
        });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format("Error reading counters [magazineIdentifier = %s]", magazineIdentifier))
                    .build();
        }
    }

    private boolean loadData(final String key, T data) throws ExecutionException, RetryException {
        return writeRetryer.call(() -> {
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

    private long incrementAndGetLoadPointer(String magazineIdentifier) throws ExecutionException, RetryException {
        Record record = readRetryer.call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            String key = Joiner.on("_").join(magazineIdentifier, Constants.POINTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.LOAD_POINTER, 1L)),
                    Operation.get(Constants.LOAD_POINTER));
        });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format("Error reading pointers [magazineIdentifier = %s]", magazineIdentifier))
                    .build();
        }
        return record.getLong(Constants.LOAD_POINTER);
    }

    private boolean alreadyExists(String data) throws ExecutionException, RetryException {
        return writeRetryer.call(() -> {
            Statement statement = new Statement();
            statement.setNamespace(namespace);
            statement.setSetName(dataSetName);
            statement.setIndexName(Constants.DATA);
            statement.setFilter(Filter.equal(Constants.DATA, data));
            RecordSet rs = aerospikeClient.query(null, statement);
            return rs.next();
        });
    }

    private void createIndex(String setName, String bin) {
        try{
            aerospikeClient.createIndex(null, namespace, setName, setName, bin, IndexType.STRING).waitTillComplete();
        }
        catch(AerospikeException e) {
            if(e.getResultCode() == 200) {
                return;
            }
            throw e;
        }
    }
}
