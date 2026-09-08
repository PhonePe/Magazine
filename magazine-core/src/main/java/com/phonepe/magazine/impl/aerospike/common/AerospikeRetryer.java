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
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.exception.MagazineExceptions;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Bounded retry for transient Aerospike failures.
 * <p>
 * Retries {@link AerospikeException} only. It must never wrap a non-idempotent operation such as
 * the fire-pointer claim, where a retry after a timeout could double-advance the pointer.
 * <p>
 * Hand-rolled rather than delegating to a retry library because this sits on every round trip: a
 * first-attempt success costs a loop entry and nothing else. It also keeps Guava off the graph.
 */
public final class AerospikeRetryer {

    private static final String EXHAUSTED = "Aerospike operation failed after %d attempts.";
    private static final String NOT_RETRYABLE = "Aerospike operation failed.";

    private final int maxAttempts;
    private final long delayMillis;

    public AerospikeRetryer() {
        this(AerospikeConstants.MAX_RETRIES, AerospikeConstants.AEROSPIKE_RETRY_DELAY_MS);
    }

    public AerospikeRetryer(final int maxAttempts, final long delayMillis) {
        this.maxAttempts = maxAttempts;
        this.delayMillis = delayMillis;
    }

    /**
     * @throws MagazineException {@code RETRIES_EXHAUSTED} when every attempt failed, or
     *         {@code CONNECTION_ERROR} for a failure that is not worth retrying. An interrupt
     *         while backing off restores the interrupt flag and reports
     *         {@code RETRIES_EXHAUSTED}, so a shutting-down consumer stops rather than spins.
     */
    public <R> R call(final Callable<R> callable) {
        AerospikeException lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return callable.call();
            } catch (AerospikeException e) {
                lastFailure = e;
            } catch (MagazineException e) {
                throw e;
            } catch (Exception e) {
                throw MagazineExceptions.connectionError(NOT_RETRYABLE, e);
            }
            if (attempt < maxAttempts) {
                sleep();
            }
        }
        throw MagazineExceptions.retriesExhausted(EXHAUSTED.formatted(maxAttempts), lastFailure);
    }

    private void sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(delayMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw MagazineExceptions.retriesExhausted("Interrupted while retrying.", e);
        }
    }
}
