package com.phonepe.growth.magazine.impl.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.github.rholder.retry.*;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.phonepe.growth.magazine.common.Constants;
import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.MagazineType;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
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
    public AerospikeStorage(AerospikeClient aerospikeClient, String namespace, String dataSetName, String metaSetName, Class<T> klass) {
        super(MagazineType.AEROSPIKE);

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
                .retryIfResult(Predicates.isNull())
                .withStopStrategy(StopStrategies.neverStop())
                .withWaitStrategy(WaitStrategies.fixedWait(Constants.DELAY_BETWEEN_RETRIES, TimeUnit.MILLISECONDS))
                .withBlockStrategy(BlockStrategies.threadSleepStrategy())
                .build();
        clazz = klass;
    }

    @Override
    public boolean prepare(String keyPrefix) {
        try {
            return writeRetryer.call(() -> {
                final WritePolicy writePolicy = aerospikeClient.getWritePolicyDefault();
                writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

                final String pointerKey = Joiner.on("_").join(keyPrefix, Constants.POINTERS);
                aerospikeClient.operate(writePolicy,
                        new Key(namespace, metaSetName, pointerKey),
                        Operation.put(new Bin(Constants.LOAD_POINTER, 0L)),
                        Operation.put(new Bin(Constants.FIRE_POINTER, 0L)));

                final String counterKey = Joiner.on("_").join(keyPrefix, Constants.COUNTERS);
                aerospikeClient.operate(writePolicy,
                        new Key(namespace, metaSetName, counterKey),
                        Operation.put(new Bin(Constants.LOAD_COUNTER, 0L)),
                        Operation.put(new Bin(Constants.FIRE_COUNTER, 0L)));
                return true;
            });
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format("Error writing pointers [keyPrefix = %s]", keyPrefix))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error writing pointers [keyPrefix = %s]", keyPrefix))
                    .build();
        }
    }

    @Override
    public boolean load(String keyPrefix, T data) {
        try {
            long loadPointer = incrementAndGetLoadPointer(keyPrefix);
            final String key = Joiner.on("_").join(keyPrefix, loadPointer);
            boolean success = loadData(key, data);
            incrementLoadCounter(keyPrefix);
            return success;
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format("Error loading data [keyPrefix = %s]", keyPrefix))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error loading data [keyPrefix = %s]", keyPrefix))
                    .build();
        }
    }

    @Override
    public boolean reload(String keyPrefix, T data){
        try {
            long loadPointer = incrementAndGetLoadPointer(keyPrefix);
            final String key = Joiner.on("_").join(keyPrefix, loadPointer);
            return loadData(key, data);
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format("Error loading data [keyPrefix = %s]", keyPrefix))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error loading data [keyPrefix = %s]", keyPrefix))
                    .build();
        }
    }

    @Override
    public Optional<T> fire(String keyPrefix) {
        return Optional.of(clazz.cast(fireWithRetry(keyPrefix).getValue(Constants.DATA)));
    }

    @Override
    public MetaData getMetaData(String keyPrefix){
        try{
            Record counterRecord = readRetryer.call(() -> {
                final String key = Joiner.on("_").join(keyPrefix, Constants.COUNTERS);
                return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, metaSetName, key));
            });

            return MetaData.builder()
                    .fireCounter(counterRecord.getLong(Constants.FIRE_COUNTER))
                    .loadCounter(counterRecord.getLong(Constants.LOAD_COUNTER))
                    .build();
        } catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format("Error loading data [keyPrefix = %s]", keyPrefix))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error loading data [keyPrefix = %s]", keyPrefix))
                    .build();
        }
    }

    private Record fireWithRetry(String keyPrefix) {
        try {
            return fireRetryer.call(() -> {
                Record pointerRecord = readRetryer.call(() -> {
                    final String key = Joiner.on("_").join(keyPrefix, Constants.POINTERS);
                    return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, metaSetName, key));
                });

                long loadPointer = pointerRecord.getLong(Constants.LOAD_POINTER);
                long firePointer = pointerRecord.getLong(Constants.FIRE_POINTER);

                if (firePointer >= loadPointer) {
                    throw MagazineException.builder()
                            .errorCode(ErrorCode.NOTHING_TO_FIRE)
                            .message(String.format("No data to fire [keyPrefix = %s]", keyPrefix))
                            .build();
                }

                Record record = readRetryer.call(() -> {
                    final WritePolicy writePolicy = aerospikeClient.getWritePolicyDefault();
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;

                    final String key = Joiner.on("_").join(keyPrefix, Constants.POINTERS);
                    return aerospikeClient.operate(writePolicy,
                            new Key(namespace, metaSetName, key),
                            Operation.add(new Bin(Constants.FIRE_POINTER, 1)),
                            Operation.get(Constants.FIRE_POINTER));
                });

                //Fire data
                return readRetryer.call(() -> {
                    String key = Joiner.on("_").join(keyPrefix, record.getLong(Constants.FIRE_POINTER));
                    return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, dataSetName, key));
                });
            });
        }
        catch (RetryException re) {
            throw MagazineException.builder()
                    .cause(re)
                    .errorCode(ErrorCode.RETRIES_EXHAUSTED)
                    .message(String.format("Error firing data [keyPrefix = %s]", keyPrefix))
                    .build();
        } catch (ExecutionException e) {
            throw MagazineException.builder()
                    .cause(e)
                    .errorCode(ErrorCode.CONNECTION_ERROR)
                    .message(String.format("Error firing data [keyPrefix = %s]", keyPrefix))
                    .build();
        }
    }


    private void incrementLoadCounter(String keyPrefix) throws ExecutionException, RetryException {
        Record record = readRetryer.call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            String key = Joiner.on("_").join(keyPrefix, Constants.COUNTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.LOAD_COUNTER, 1L)),
                    Operation.get(Constants.LOAD_COUNTER));
        });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format("Error reading counters [keyPrefix = %s]", keyPrefix))
                    .build();
        }
    }

    private boolean loadData(final String key, T data) throws ExecutionException, RetryException {
        return writeRetryer.call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            aerospikeClient.put(writePolicy,
                    new Key(namespace, dataSetName, key),
                    new Bin(Constants.DATA, data),
                    new Bin(Constants.MODIFIED_AT, System.currentTimeMillis()));
            return true;
        });
    }

    private long incrementAndGetLoadPointer(String keyPrefix) throws ExecutionException, RetryException {
        Record record = readRetryer.call(() -> {
            final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            String key = Joiner.on("_").join(keyPrefix, Constants.POINTERS);
            return aerospikeClient.operate(writePolicy,
                    new Key(namespace, metaSetName, key),
                    Operation.add(new Bin(Constants.LOAD_POINTER, 1L)),
                    Operation.get(Constants.LOAD_POINTER));
        });

        if (record == null) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message(String.format("Error reading pointers [keyPrefix = %s]", keyPrefix))
                    .build();
        }
        return record.getLong(Constants.LOAD_POINTER);
    }
}
