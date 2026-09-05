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

package com.phonepe.magazine.impl.aerospike.common;

import com.aerospike.client.AerospikeException;
import com.github.rholder.retry.BlockStrategies;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Getter;

/**
 * Bounded retry for transient Aerospike failures.
 * <p>
 * Note this retries {@link AerospikeException} only - it must never wrap a non-idempotent
 * operation such as the fire-pointer claim, where a retry after a timeout could double-advance
 * the pointer.
 */
public class AerospikeRetryerFactory {

    @Getter
    private final Retryer<Object> retryer;

    public AerospikeRetryerFactory() {
        this.retryer = RetryerBuilder.newBuilder()
                .retryIfExceptionOfType(AerospikeException.class)
                .withStopStrategy(StopStrategies.stopAfterAttempt(AerospikeConstants.MAX_RETRIES))
                .withWaitStrategy(WaitStrategies.fixedWait(AerospikeConstants.AEROSPIKE_RETRY_DELAY_MS, TimeUnit.MILLISECONDS))
                .withBlockStrategy(BlockStrategies.threadSleepStrategy())
                .build();
    }

    /**
     * Type-preserving wrapper over {@link Retryer#call(Callable)}, so callers do not each repeat
     * an unchecked cast of the result.
     */
    @SuppressWarnings("unchecked")
    public <R> R call(final Callable<R> callable) throws ExecutionException, RetryException {
        return (R) retryer.call(callable::call);
    }
}
