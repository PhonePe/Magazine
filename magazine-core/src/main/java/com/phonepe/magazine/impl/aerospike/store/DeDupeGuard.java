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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikeNaming;
import com.phonepe.magazine.impl.aerospike.common.AerospikePolicies;
import com.phonepe.magazine.metrics.MagazineMetrics;
import com.phonepe.magazine.metrics.StorageOperation;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Guards a load against duplicate payloads.
 * <p>
 * The claim is a single write under {@link RecordExistsAction#CREATE_ONLY}, and that write
 * <em>is</em> the mutual exclusion: the server admits exactly one creator of a key and rejects the
 * rest with {@code KEY_EXISTS_ERROR}. Earlier releases spent four round trips on a distributed
 * lock to express what the storage engine already guarantees in one.
 * <p>
 * The marker is written <em>before</em> the payload, so it must be withdrawn if the load does not
 * complete; otherwise a failed write suppresses a legitimate retry until the TTL elapses. That is
 * why callers must use try-with-resources and call {@link Handle#confirm()} on success.
 */
public interface DeDupeGuard<T> {

    /**
     * Claim exclusive ownership of this payload for the duration of a load.
     *
     * @return a handle reporting whether the payload had already been claimed.
     */
    Handle claim(String magazineIdentifier, T data);

    interface Handle extends AutoCloseable {

        /** @return true if this payload was already loaded and must not be loaded again. */
        boolean duplicate();

        /** Confirm the payload was durably written, making the suppression permanent. */
        void confirm();

        /** Withdraws an unconfirmed claim so a retry is not suppressed by a failed load. */
        @Override
        void close();
    }

    static <T> DeDupeGuard<T> disabled() {
        return (magazineIdentifier, data) -> NoOpHandle.INSTANCE;
    }

    static <T> DeDupeGuard<T> aerospike(final IAerospikeClient client,
            final MagazineMetrics metrics,
            final String namespace,
            final String farmId,
            final String clientId,
            final MagazineScope scope,
            final int recordTtl) {
        return new Aerospike<>(client, metrics, namespace,
                AerospikeNaming.resolveSetName(AerospikeNaming.deDuperSetName(clientId), farmId, scope),
                recordTtl);
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    final class NoOpHandle implements Handle {

        private static final NoOpHandle INSTANCE = new NoOpHandle();

        @Override
        public boolean duplicate() {
            return false;
        }

        @Override
        public void confirm() {
            // deduplication disabled - nothing to record
        }

        @Override
        public void close() {
            // nothing was claimed
        }
    }

    /**
     * Deduplication backed by a marker record whose TTL matches the data record's, so the
     * suppression window and the data lifetime expire together.
     */
    @Slf4j
    final class Aerospike<T> implements DeDupeGuard<T> {

        private final IAerospikeClient client;
        private final MagazineMetrics metrics;
        private final String namespace;
        private final String deDuperSetName;
        private final WritePolicy claimPolicy;
        private final WritePolicy withdrawPolicy;

        private Aerospike(final IAerospikeClient client,
                final MagazineMetrics metrics,
                final String namespace,
                final String deDuperSetName,
                final int recordTtl) {
            this.client = client;
            this.metrics = metrics;
            this.namespace = namespace;
            this.deDuperSetName = deDuperSetName;

            this.claimPolicy = AerospikePolicies.writePolicy(client);
            this.claimPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            this.claimPolicy.expiration = recordTtl;
            this.claimPolicy.sendKey = false;
            this.withdrawPolicy = AerospikePolicies.writePolicy(client);
        }

        @Override
        public Handle claim(final String magazineIdentifier, final T data) {
            final Key key = new Key(namespace, deDuperSetName, magazineIdentifier + data);

            metrics.aerospikeCall(magazineIdentifier, StorageOperation.CLAIM_DEDUPE_MARKER);
            try {
                // A record needs at least one bin to exist at all, and the claim instant is the
                // one fact worth carrying: it dates the start of the suppression window.
                client.put(claimPolicy, key,
                        new Bin(AerospikeConstants.MODIFIED_AT, System.currentTimeMillis()));
            } catch (AerospikeException e) {
                if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                    metrics.dedupeOutcome(magazineIdentifier, MagazineMetrics.DedupeOutcome.DUPLICATE);
                    return NoOpDuplicateHandle.INSTANCE;
                }
                throw e;
            }
            metrics.dedupeOutcome(magazineIdentifier, MagazineMetrics.DedupeOutcome.CLAIMED);
            return new ClaimedHandle(magazineIdentifier, key);
        }

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        private static final class NoOpDuplicateHandle implements Handle {

            private static final NoOpDuplicateHandle INSTANCE = new NoOpDuplicateHandle();

            @Override
            public boolean duplicate() {
                return true;
            }

            @Override
            public void confirm() {
                // the payload was suppressed, not loaded
            }

            @Override
            public void close() {
                // we did not create the marker, so we must not remove it
            }
        }

        private final class ClaimedHandle implements Handle {

            private final String magazineIdentifier;
            private final Key key;
            private boolean confirmed;

            private ClaimedHandle(final String magazineIdentifier, final Key key) {
                this.magazineIdentifier = magazineIdentifier;
                this.key = key;
            }

            @Override
            public boolean duplicate() {
                return false;
            }

            @Override
            public void confirm() {
                this.confirmed = true;
            }

            /**
             * Best effort by design. If the withdrawal itself fails the marker simply lives out
             * its TTL, suppressing retries of this payload until then - degraded, but never
             * incorrect, and preferable to failing a load that already succeeded.
             */
            @Override
            public void close() {
                if (confirmed) {
                    return;
                }
                try {
                    metrics.aerospikeCall(magazineIdentifier, StorageOperation.WITHDRAW_DEDUPE_MARKER);
                    client.delete(withdrawPolicy, key);
                } catch (Exception e) {
                    log.warn("Could not withdraw the deduplication marker for magazine {}; "
                            + "retries of this payload stay suppressed until it expires.",
                            magazineIdentifier, e);
                }
            }
        }
    }
}
