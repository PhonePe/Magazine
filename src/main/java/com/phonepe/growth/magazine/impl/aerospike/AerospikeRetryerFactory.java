package com.phonepe.growth.magazine.impl.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import com.github.rholder.retry.*;
import com.google.common.base.Predicates;
import com.phonepe.growth.magazine.common.Constants;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

@Getter
public class AerospikeRetryerFactory<T> {
    private final Retryer<T> retryer;
    private final Retryer<Record> fireRetryer;

    public AerospikeRetryerFactory() {
        retryer = RetryerBuilder.<T>newBuilder()
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
    }
}
