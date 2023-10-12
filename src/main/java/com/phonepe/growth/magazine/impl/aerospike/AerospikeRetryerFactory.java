package com.phonepe.growth.magazine.impl.aerospike;

import com.aerospike.client.AerospikeException;
import com.github.rholder.retry.BlockStrategies;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.phonepe.growth.magazine.common.Constants;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.Getter;

@Getter
public class AerospikeRetryerFactory<T> {
    private final Retryer<T> retryer;
    private final Retryer<Object> fireRetryer;

    public AerospikeRetryerFactory() {
        retryer = RetryerBuilder.<T>newBuilder()
                .retryIfExceptionOfType(AerospikeException.class)
                .withStopStrategy(StopStrategies.stopAfterAttempt(Constants.MAX_RETRIES))
                .withWaitStrategy(WaitStrategies.fixedWait(Constants.DELAY_BETWEEN_RETRIES, TimeUnit.MILLISECONDS))
                .withBlockStrategy(BlockStrategies.threadSleepStrategy())
                .build();
        fireRetryer = RetryerBuilder.newBuilder()
                .retryIfExceptionOfType(AerospikeException.class)
                .retryIfResult(Objects::isNull)
                .withStopStrategy(StopStrategies.neverStop())
                .withWaitStrategy(WaitStrategies.fixedWait(Constants.DELAY_BETWEEN_RETRIES, TimeUnit.MILLISECONDS))
                .withBlockStrategy(BlockStrategies.threadSleepStrategy())
                .build();
    }
}
