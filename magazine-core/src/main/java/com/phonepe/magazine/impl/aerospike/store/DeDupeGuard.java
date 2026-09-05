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

package com.phonepe.magazine.impl.aerospike.store;

import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikeRetryerFactory;
import static com.phonepe.dlm.exception.ErrorCode.LOCK_UNAVAILABLE;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.github.rholder.retry.RetryException;
import com.phonepe.dlm.DistributedLockManager;
import com.phonepe.dlm.lock.base.LockBase;
import com.phonepe.dlm.lock.mode.LockMode;
import com.phonepe.dlm.lock.storage.aerospike.AerospikeStore;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.impl.aerospike.common.AerospikeNaming;
import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.lock.Lock;
import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineExceptions;
import java.util.concurrent.ExecutionException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Guards a load against duplicate payloads.
 * <p>
 * Modelled as a scope so callers never branch on whether deduplication is enabled - when it is
 * off, {@link #disabled()} hands back a handle whose operations are no-ops. The handle is
 * {@link AutoCloseable} so the distributed lock is always released on the way out.
 */
public interface DeDupeGuard<T> {

    /**
     * Acquire exclusive access for this payload.
     *
     * @throws com.phonepe.magazine.exception.MagazineException with
     *         {@code ACTION_DENIED_PARALLEL_ATTEMPT} if another writer holds the lock.
     */
    Handle acquire(String magazineIdentifier, T data);

    interface Handle extends AutoCloseable {

        /** @return true if this payload was already loaded and must not be loaded again. */
        boolean alreadyLoaded() throws ExecutionException, RetryException;

        /** Record this payload so a later load of the same value is suppressed. */
        void remember() throws ExecutionException, RetryException;

        @Override
        void close();
    }

    static <T> DeDupeGuard<T> disabled() {
        return (magazineIdentifier, data) -> NoOpHandle.INSTANCE;
    }

    /**
     * Builds a guard backed by a distributed lock, wiring the lock manager itself so DLM types stay
     * inside this package.
     */
    static <T> DeDupeGuard<T> aerospike(final IAerospikeClient client,
            final AerospikeRetryerFactory retryerFactory,
            final String namespace,
            final String farmId,
            final String clientId,
            final MagazineScope scope,
            final int recordTtl) {
        final DistributedLockManager lockManager = new DistributedLockManager(
                AerospikeConstants.DLM_CLIENT_ID, farmId,
                LockBase.builder()
                        .mode(LockMode.EXCLUSIVE)
                        .lockStore(AerospikeStore.builder()
                                .aerospikeClient(client)
                                .namespace(namespace)
                                .setSuffix(AerospikeConstants.MAGAZINE_DISTRIBUTED_LOCK_SET_NAME_SUFFIX)
                                .build())
                        .build());
        lockManager.initialize();
        return new Aerospike<>(client, retryerFactory, lockManager,
                AerospikeNaming.resolveLockLevel(scope), namespace,
                AerospikeNaming.resolveSetName(AerospikeNaming.deDuperSetName(clientId), farmId, scope),
                recordTtl);
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    final class NoOpHandle implements Handle {

        private static final NoOpHandle INSTANCE = new NoOpHandle();

        @Override
        public boolean alreadyLoaded() {
            return false;
        }

        @Override
        public void remember() {
            // deduplication disabled - nothing to record
        }

        @Override
        public void close() {
            // no lock was taken
        }
    }

    /**
     * Deduplication backed by a distributed lock plus a marker record whose TTL matches the data
     * record's, so the suppression window and the data lifetime expire together.
     */
    @Slf4j
    final class Aerospike<T> implements DeDupeGuard<T> {

        private final IAerospikeClient client;
        private final AerospikeRetryerFactory retryerFactory;
        private final DistributedLockManager lockManager;
        private final LockLevel lockLevel;
        private final String namespace;
        private final String deDuperSetName;
        private final int recordTtl;

        private Aerospike(final IAerospikeClient client,
                final AerospikeRetryerFactory retryerFactory,
                final DistributedLockManager lockManager,
                final LockLevel lockLevel,
                final String namespace,
                final String deDuperSetName,
                final int recordTtl) {
            this.client = client;
            this.retryerFactory = retryerFactory;
            this.lockManager = lockManager;
            this.lockLevel = lockLevel;
            this.namespace = namespace;
            this.deDuperSetName = deDuperSetName;
            this.recordTtl = recordTtl;
        }


        @Override
        public Handle acquire(final String magazineIdentifier, final T data) {
            final Lock lock = lockManager.getLockInstance(
                    String.join(AerospikeConstants.KEY_DELIMITER, magazineIdentifier, data.toString()), lockLevel);
            try {
                lockManager.tryAcquireLock(lock);
            } catch (DLMException e) {
                if (LOCK_UNAVAILABLE.equals(e.getErrorCode())) {
                    throw MagazineExceptions.of(ErrorCode.ACTION_DENIED_PARALLEL_ATTEMPT,
                            String.format("Error acquiring lock - %s", lock.getLockId()), e);
                }
                throw e;
            }
            return new AcquiredHandle(lock, key(magazineIdentifier, data));
        }

        private Key key(final String magazineIdentifier, final T data) {
            return new Key(namespace, deDuperSetName, magazineIdentifier + data);
        }

        private final class AcquiredHandle implements Handle {

            private final Lock lock;
            private final Key deDuperKey;

            private AcquiredHandle(final Lock lock, final Key deDuperKey) {
                this.lock = lock;
                this.deDuperKey = deDuperKey;
            }

            @Override
            public boolean alreadyLoaded() throws ExecutionException, RetryException {
                return retryerFactory.call(
                        () -> client.exists(client.getReadPolicyDefault(), deDuperKey));
            }

            @Override
            public void remember() throws ExecutionException, RetryException {
                retryerFactory.call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
                    writePolicy.expiration = recordTtl;
                    writePolicy.sendKey = false;
                    client.put(writePolicy, deDuperKey,
                            new Bin(AerospikeConstants.MODIFIED_AT, System.currentTimeMillis()));
                    return true;
                });
            }

            @Override
            public void close() {
                try {
                    lockManager.releaseLock(lock);
                } catch (Exception e) {
                    log.warn("Error releasing deduplication lock for lock id {}", lock.getLockId(), e);
                }
            }
        }
    }
}
