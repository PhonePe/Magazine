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

package com.phonepe.magazine.impl.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.exception.MagazineExceptions;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikeNaming;
import com.phonepe.magazine.impl.aerospike.common.AerospikeRetryer;
import com.phonepe.magazine.impl.aerospike.common.ErrorMessage;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves the persisted per-magazine configuration - shard count and metadata schema version -
 * creating it on first use.
 * <p>
 * The persisted shard count is authoritative. The storage's configured shard count is only a
 * creation default; when a magazine already exists its stored count is adopted, which is what
 * allows one storage instance to serve magazines with differing shard counts.
 */
@Slf4j
@RequiredArgsConstructor
final class AerospikeMagazineInitializer {

    private final IAerospikeClient client;
    private final AerospikeRetryer retryerFactory;
    private final String namespace;
    private final String metaSetName;
    /** Shard count applied only when creating a magazine that does not yet exist. */
    private final int configuredShards;
    private final boolean allowShardIncrease;

    MagazineContext initialize(final String magazineIdentifier) {
        try {
            return resolve(magazineIdentifier);
        } catch (MagazineException e) {
            throw e;
        } catch (Exception e) {
            throw MagazineExceptions.connectionError(
                    String.format(ErrorMessage.ERROR_INITIALIZING_MAGAZINE, magazineIdentifier), e);
        }
    }

    private MagazineContext resolve(final String magazineIdentifier) {
        final Key shardConfigurationKey = new Key(namespace, metaSetName,
                AerospikeNaming.shardConfigurationName(magazineIdentifier));
        Record shardConfiguration = read(shardConfigurationKey);

        if (Objects.isNull(shardConfiguration)) {
            final WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
            writePolicy.expiration = AerospikeConstants.SHARD_CONFIGURATION_TTL_SECONDS;
            writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            final InitializationResult result = retryerFactory
                    .call(() -> createOrRead(writePolicy, shardConfigurationKey));
            if (result.created()) {
                return new MagazineContext(magazineIdentifier,
                        AerospikeConstants.UNIFIED_METADATA_SCHEMA_VERSION, configuredShards);
            }
            shardConfiguration = result.record();
        }

        if (Objects.isNull(shardConfiguration)) {
            throw MagazineExceptions.magazineUnprepared("Magazine shard configuration is unavailable.");
        }

        final int schemaVersion = shardConfiguration.getInt(AerospikeConstants.METADATA_SCHEMA_VERSION);
        if (schemaVersion != AerospikeConstants.LEGACY_METADATA_SCHEMA_VERSION
                && schemaVersion != AerospikeConstants.UNIFIED_METADATA_SCHEMA_VERSION) {
            throw MagazineExceptions.invalidConfiguration(
                    "Unsupported magazine metadata schema version: " + schemaVersion);
        }

        final int storedShards = shardConfiguration.getInt(AerospikeConstants.SHARDS_BIN);
        final int effectiveShards = reconcileShards(shardConfigurationKey,
                magazineIdentifier, storedShards, schemaVersion);
        return new MagazineContext(magazineIdentifier, schemaVersion, effectiveShards);
    }

    /**
     * The persisted shard count wins unless an increase is explicitly requested and permitted.
     * A mismatch is never fatal - adopting the stored layout is always correct, whereas rewriting
     * it on every boot silently mutates shared state.
     */
    private int reconcileShards(final Key shardConfigurationKey,
            final String magazineIdentifier,
            final int storedShards,
            final int schemaVersion) {
        if (configuredShards == storedShards) {
            return storedShards;
        }
        if (configuredShards < storedShards) {
            log.info("Magazine {} is persisted with {} shards; storage is configured for {}. "
                            + "Adopting the persisted count - shards cannot be decreased.",
                    magazineIdentifier, storedShards, configuredShards);
            return storedShards;
        }
        if (!allowShardIncrease) {
            log.info("Magazine {} is persisted with {} shards; storage is configured for {}. "
                            + "Adopting the persisted count - set allowShardIncrease to widen it.",
                    magazineIdentifier, storedShards, configuredShards);
            return storedShards;
        }
        if (storedShards <= 1) {
            // Unsharded keys carry no SHARD_<n> fragment, so anything still queued under the flat
            // layout becomes unaddressable the moment the magazine turns sharded. Refuse rather
            // than strand it.
            verifyUnshardedMagazineIsDrained(magazineIdentifier, schemaVersion);
            increaseShards(shardConfigurationKey, storedShards, configuredShards);
            log.warn("Magazine {} promoted from unsharded to {} shards. It was verified drained "
                            + "first; already-consumed records under the flat key layout are "
                            + "abandoned and will expire with their TTL.",
                    magazineIdentifier, configuredShards);
            return configuredShards;
        }
        increaseShards(shardConfigurationKey, storedShards, configuredShards);
        log.info("Magazine {} shard count increased from {} to {}.",
                magazineIdentifier, storedShards, configuredShards);
        return configuredShards;
    }

    /**
     * A promotion is only safe once every loaded record has been fired: the flat-key records are
     * unreachable under a sharded layout, so undelivered data would be silently lost.
     */
    private void verifyUnshardedMagazineIsDrained(final String magazineIdentifier,
            final int schemaVersion) {
        final String suffix = schemaVersion == AerospikeConstants.UNIFIED_METADATA_SCHEMA_VERSION
                ? AerospikeConstants.METADATA
                : AerospikeConstants.POINTERS;
        final Record pointers = read(new Key(namespace, metaSetName,
                magazineIdentifier + AerospikeConstants.KEY_DELIMITER + suffix));
        if (Objects.isNull(pointers)) {
            return; // nothing was ever written under the flat layout
        }
        final long undelivered = pointers.getLong(AerospikeConstants.LOAD_POINTER)
                - pointers.getLong(AerospikeConstants.FIRE_POINTER);
        if (undelivered > 0) {
            throw MagazineExceptions.invalidShards(String.format(
                    "Cannot promote unsharded magazine %s to %d shards: %d record(s) are still "
                            + "undelivered and would become unreachable under the sharded key "
                            + "layout. Drain the magazine first.",
                    magazineIdentifier, configuredShards, undelivered));
        }
    }

    private Record read(final Key key) {
        return retryerFactory.call(() -> client.get(client.getReadPolicyDefault(), key));
    }

    private InitializationResult createOrRead(final WritePolicy writePolicy, final Key key) {
        try {
            client.put(writePolicy, key,
                    new Bin(AerospikeConstants.SHARDS_BIN, configuredShards),
                    new Bin(AerospikeConstants.METADATA_SCHEMA_VERSION, AerospikeConstants.UNIFIED_METADATA_SCHEMA_VERSION),
                    new Bin(AerospikeConstants.CREATED_AT, System.currentTimeMillis()));
            return new InitializationResult(true, null);
        } catch (AerospikeException e) {
            final Record record = client.get(client.getReadPolicyDefault(), key);
            if (Objects.isNull(record)) {
                throw e;
            }
            return new InitializationResult(false, record);
        }
    }

    private void increaseShards(final Key key,
            final int expectedShards,
            final int targetShards) {
        retryerFactory.call(() -> {
            final WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
                    writePolicy.failOnFilteredOut = true;
                    writePolicy.filterExp = Exp.build(Exp.eq(
                            Exp.intBin(AerospikeConstants.SHARDS_BIN), Exp.val(expectedShards)));
                    writePolicy.expiration = AerospikeConstants.SHARD_CONFIGURATION_TTL_SECONDS;
                    try {
                        return client.operate(writePolicy, key,
                                Operation.put(new Bin(AerospikeConstants.SHARDS_BIN, targetShards)),
                                Operation.get());
                    } catch (AerospikeException e) {
                        if (e.getResultCode() != ResultCode.FILTERED_OUT) {
                            throw e;
                        }
                        final Record record = client.get(client.getReadPolicyDefault(), key);
                        if (Objects.nonNull(record)
                                && record.getInt(AerospikeConstants.SHARDS_BIN) == targetShards) {
                            return record;
                        }
                        throw MagazineExceptions.invalidShards(
                                "Magazine shard configuration changed concurrently.");
                    }
                });
    }

    private record InitializationResult(boolean created, Record record) {
    }
}
