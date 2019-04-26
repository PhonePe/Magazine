package com.phonepe.growth.magazine.impl.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.github.rholder.retry.*;
import com.google.common.base.Joiner;
import com.phonepe.growth.magazine.common.Constants;
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
public class AerospikeStorage extends BaseMagazineStorage {

    private final AerospikeClient aerospikeClient;
    private final String namespace;
    private final String dataSetName;
    private final String metaSetName;
    private final Retryer<Boolean> writeRetryer;
    private final Retryer<Record> readRetryer;

    @Builder
    public AerospikeStorage(AerospikeClient aerospikeClient, String namespace, String dataSetName, String metaSetName) {
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
    }

    @Override
    public boolean prepare(String keyPrefix) {
        try {
            return writeRetryer.call(() -> {
                final WritePolicy writePolicy = aerospikeClient.getWritePolicyDefault();
                writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

                final String key = Joiner.on("_").join(keyPrefix, Constants.POINTERS);
                aerospikeClient.operate(writePolicy,
                        new Key(namespace, metaSetName, key),
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

    @Override
    public boolean load(String keyPrefix, Object data) {
        try {
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

            return writeRetryer.call(() -> {
                final WritePolicy writePolicy = new WritePolicy(aerospikeClient.getWritePolicyDefault());
                writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
                final String key = Joiner.on("_").join(keyPrefix, record.getLong(Constants.LOAD_POINTER));
                aerospikeClient.put(writePolicy,
                        new Key(namespace, dataSetName, key),
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

    @Override
    public Optional<Object> fire(String keyPrefix) {
        try {
            Record pointerRecord = readRetryer.call(() -> {
                final String key = Joiner.on("_").join(keyPrefix, Constants.POINTERS);
                return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, metaSetName, key));
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
                        new Key(namespace, metaSetName, key),
                        Operation.add(new Bin(Constants.FIRE_POINTER, 1)),
                        Operation.get(Constants.FIRE_POINTER));
            });

            //Fire data
            Record firedRecord = readRetryer.call(() -> {
                String key = Joiner.on("_").join(keyPrefix, record.getLong(Constants.FIRE_POINTER));
                return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(namespace, dataSetName, key));
            });

            return null == firedRecord ? Optional.empty() : Optional.of(firedRecord.getValue(Constants.DATA));
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
}
