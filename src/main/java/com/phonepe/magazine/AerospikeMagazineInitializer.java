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

package com.phonepe.magazine;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.github.rholder.retry.RetryException;
import com.phonepe.magazine.common.Constants;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.impl.aerospike.AerospikeStorage;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class AerospikeMagazineInitializer {

    static int initialize(final AerospikeStorage<?> storage,
            final String magazineIdentifier) throws ExecutionException, RetryException {
        final Key shardConfigurationKey = new Key(storage.getNamespace(), storage.getMetaSetName(),
                magazineIdentifier + Constants.KEY_DELIMITER + Constants.SHARDS_BIN);
        Record shardConfiguration = read(storage, shardConfigurationKey);

        if (Objects.isNull(shardConfiguration)) {
            final WritePolicy writePolicy = new WritePolicy(storage.getAerospikeClient().getWritePolicyDefault());
            writePolicy.expiration = Constants.SHARD_CONFIGURATION_TTL_SECONDS;
            writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            final InitializationResult result = (InitializationResult) storage.getRetryerFactory()
                    .getRetryer()
                    .call(() -> createOrRead(storage, writePolicy, shardConfigurationKey));
            if (result.created()) {
                return Constants.UNIFIED_METADATA_SCHEMA_VERSION;
            }
            shardConfiguration = result.record();
        }

        if (Objects.isNull(shardConfiguration)) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.MAGAZINE_UNPREPARED)
                    .message("Magazine shard configuration is unavailable.")
                    .build();
        }
        final int storedShards = shardConfiguration.getInt(Constants.SHARDS_BIN);
        if (storedShards > storage.getShards()) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.INVALID_SHARDS)
                    .message("Cannot decrease shards of a magazine.")
                    .build();
        }
        if (storedShards <= 1 && storage.getShards() > 1) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.INVALID_SHARDS)
                    .message("Cannot convert unsharded to sharded magazine.")
                    .build();
        }
        final int schemaVersion = (int) shardConfiguration.getLong(Constants.METADATA_SCHEMA_VERSION);
        if (schemaVersion != Constants.LEGACY_METADATA_SCHEMA_VERSION
                && schemaVersion != Constants.UNIFIED_METADATA_SCHEMA_VERSION) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.INVALID_CONFIGURATION)
                    .message("Unsupported magazine metadata schema version: " + schemaVersion)
                    .build();
        }
        if (storedShards < storage.getShards()) {
            increaseShards(storage, shardConfigurationKey, storedShards);
        }
        return schemaVersion;
    }

    private static Record read(final AerospikeStorage<?> storage, final Key key)
            throws ExecutionException, RetryException {
        return (Record) storage.getRetryerFactory()
                .getRetryer()
                .call(() -> storage.getAerospikeClient().get(
                        storage.getAerospikeClient().getReadPolicyDefault(), key));
    }

    private static InitializationResult createOrRead(final AerospikeStorage<?> storage,
            final WritePolicy writePolicy,
            final Key key) {
        try {
            storage.getAerospikeClient().put(writePolicy, key,
                    new Bin(Constants.SHARDS_BIN, storage.getShards()),
                    new Bin(Constants.METADATA_SCHEMA_VERSION,
                            Constants.UNIFIED_METADATA_SCHEMA_VERSION),
                    new Bin(Constants.CREATED_AT, System.currentTimeMillis()));
            return new InitializationResult(true, null);
        } catch (AerospikeException e) {
            final Record record = storage.getAerospikeClient().get(
                    storage.getAerospikeClient().getReadPolicyDefault(), key);
            if (Objects.isNull(record)) {
                throw e;
            }
            return new InitializationResult(false, record);
        }
    }

    private record InitializationResult(boolean created, Record record) {
    }

    private static Record increaseShards(final AerospikeStorage<?> storage,
            final Key key,
            final int expectedShards) throws ExecutionException, RetryException {
        return (Record) storage.getRetryerFactory()
                .getRetryer()
                .call(() -> {
                    final WritePolicy writePolicy = new WritePolicy(
                            storage.getAerospikeClient().getWritePolicyDefault());
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
                    writePolicy.failOnFilteredOut = true;
                    writePolicy.filterExp = Exp.build(Exp.eq(
                            Exp.intBin(Constants.SHARDS_BIN), Exp.val(expectedShards)));
                    writePolicy.expiration = Constants.SHARD_CONFIGURATION_TTL_SECONDS;
                    try {
                        return storage.getAerospikeClient().operate(writePolicy, key,
                                Operation.put(new Bin(Constants.SHARDS_BIN, storage.getShards())),
                                Operation.get());
                    } catch (AerospikeException e) {
                        if (e.getResultCode() != ResultCode.FILTERED_OUT) {
                            throw e;
                        }
                        final Record record = storage.getAerospikeClient().get(
                                storage.getAerospikeClient().getReadPolicyDefault(), key);
                        if (Objects.nonNull(record)
                                && record.getInt(Constants.SHARDS_BIN) == storage.getShards()) {
                            return record;
                        }
                        throw MagazineException.builder()
                                .errorCode(ErrorCode.INVALID_SHARDS)
                                .message("Magazine shard configuration changed concurrently.")
                                .build();
                    }
                });
    }
}
