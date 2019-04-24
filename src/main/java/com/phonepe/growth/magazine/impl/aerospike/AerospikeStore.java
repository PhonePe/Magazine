package com.phonepe.growth.magazine.impl.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.github.rholder.retry.*;
import com.google.common.base.Joiner;
import com.phonepe.growth.magazine.common.Constants;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import lombok.Builder;
import lombok.Data;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Data
public class AerospikeStore {
    private final AerospikeClient aerospikeClient;
    private final String namespace;
    private final String setName;
    private final Retryer<Boolean> writeRetryer;
    private final Retryer<Record> readRetryer;

    @Builder
    public AerospikeStore(final AerospikeClient aerospikeClient, String namespace, String setName) {
        this.aerospikeClient = aerospikeClient;
        this.namespace = namespace;
        this.setName = setName;
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
    }

    public boolean loadDataToAerospike(String keyPrefix, Object data) {
        try {
            Record record = readRetryer.call(() -> {
                final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
                String key = Joiner.on("_").join(keyPrefix, Constants.POINTERS);
                return aerospikeClient.operate(writePolicy,
                        new Key(namespace, setName, key),
                        Operation.add(new Bin(Constants.LOAD_POINTER, 1L)),
                        Operation.get(Constants.LOAD_POINTER));
            });

            if (record == null) {
                throw MagazineException.builder()
                        .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                        .message(String.format("Error reading pointers [keyPrefix = %s]", keyPrefix))
                        .build();
            }

            return writeRetryer.call(() -> {
                final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
                final String key = Joiner.on("_").join(keyPrefix, record.getLong(Constants.LOAD_POINTER));
                aerospikeClient.put(writePolicy,
                        new Key(namespace, setName, key),
                        new Bin(Constants.DATA, data),
                        new Bin(Constants.MODIFIED_AT, System.currentTimeMillis()));
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

    public Optional<Object> fireDataFromAerospike(String keyPrefix) {
        try {
            Record pointerRecord = readRetryer.call(() -> {
                final String key = Joiner.on("_").join(keyPrefix, Constants.POINTERS);
                return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, setName, key));
            });

            long loadPointer = pointerRecord.getLong(Constants.LOAD_POINTER);
            long firePointer = pointerRecord.getLong(Constants.FIRE_POINTER);

            if(firePointer >= loadPointer) {
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
                        new Key(namespace, setName, key),
                        Operation.add(new Bin(Constants.FIRE_POINTER, 1)),
                        Operation.get(Constants.FIRE_POINTER));
            });

            //Fire data
            Record firedRecord = readRetryer.call(() -> {
                String key = Joiner.on("_").join(keyPrefix, record.getLong(Constants.FIRE_POINTER));
                return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, setName, key));
            });

            return null == firedRecord ? Optional.empty() : Optional.of(record.getValue(Constants.DATA));
        } catch (RetryException re) {
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

    public boolean initPointers(String keyPrefix) {
        try {
            return writeRetryer.call(() -> {
                final WritePolicy writePolicy = aerospikeClient.getWritePolicyDefault();
                writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

                final String key = Joiner.on("_").join(keyPrefix, Constants.POINTERS);
                aerospikeClient.operate(writePolicy,
                        new Key(namespace, setName, key),
                        Operation.put(new Bin(Constants.LOAD_POINTER, 0L)),
                        Operation.put(new Bin(Constants.FIRE_POINTER, 0L)));
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
}
