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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.github.rholder.retry.RetryException;
import com.phonepe.magazine.common.Constants;
import com.phonepe.magazine.common.MagazineData;
import com.phonepe.magazine.common.MetaData;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.core.StorageTypeVisitor;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.impl.aerospike.AerospikeStorage;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.Builder;
import lombok.Data;

@Data
public class Magazine<T> {

    private final BaseMagazineStorage<T> baseMagazineStorage;
    private final String magazineIdentifier;

    @Builder
    public Magazine(final BaseMagazineStorage<T> baseMagazineStorage,
            final String magazineIdentifier) throws ExecutionException, RetryException {
        this.magazineIdentifier = magazineIdentifier;
        this.baseMagazineStorage = baseMagazineStorage;
        validateStorage(baseMagazineStorage);
    }

    /**
     * Load data into the specified magazine.
     *
     * @param data The data to be loaded.
     * @return True if the data was successfully loaded, false otherwise.
     */
    public boolean load(final T data) {
        return baseMagazineStorage.load(magazineIdentifier, data);
    }

    /**
     * Reload data into the specified magazine. This won't increase the load counter as the data was already loaded,
     * but load pointer will be incremented as the data will appended at the end.
     *
     * @param data The data to be reloaded.
     * @return True if the data was successfully reloaded, false otherwise.
     */
    public boolean reload(final T data) {
        return baseMagazineStorage.reload(magazineIdentifier, data);
    }

    /**
     * Fire and retrieve data from the specified magazine.
     *
     * @return The MagazineData containing the fired data.
     */
    public MagazineData<T> fire() {
        return baseMagazineStorage.fire(magazineIdentifier);
    }

    /**
     * Delete the provided MagazineData from the magazine.
     *
     * @param magazineData The MagazineData to be deleted.
     */
    public void delete(final MagazineData<T> magazineData) {
        baseMagazineStorage.delete(magazineData);
    }

    /**
     * Retrieve metadata of the specified magazine i.e the number of loaded or fired, pointers and counters.
     *
     * @return A map containing metadata information.
     */
    public Map<String, MetaData> getMetaData() {
        return baseMagazineStorage.getMetaData(magazineIdentifier);
    }

    /**
     * Peek data from specific shards and pointers within the magazine.
     *
     * @param shardPointersMap A map where keys are shard identifiers and values are sets of pointers to peek from.
     * @return A set of MagazineData containing the peeked data.
     */
    public Set<MagazineData<T>> peek(final Map<Integer, Set<Long>> shardPointersMap) {
        return baseMagazineStorage.peek(magazineIdentifier, shardPointersMap);
    }

    @SuppressWarnings("unchecked")
    private void validateStorage(final BaseMagazineStorage<T> baseMagazineStorage)
            throws ExecutionException, RetryException {
        baseMagazineStorage.getType()
                .accept(new StorageTypeVisitor<Boolean>() {
                    @Override
                    public Boolean visitAerospike() throws ExecutionException, RetryException {
                        final AerospikeStorage<T> storage = (AerospikeStorage) baseMagazineStorage;

                        final Record magazineRecord = (Record) storage.getRetryerFactory()
                                .getRetryer()
                                .call(() ->
                                        storage.getAerospikeClient()
                                                .get(storage.getAerospikeClient()
                                                                .getReadPolicyDefault(),
                                                        new Key(storage.getNamespace(),
                                                                storage.getMetaSetName(),
                                                                String.join(Constants.KEY_DELIMITER, magazineIdentifier,
                                                                        Constants.SHARDS_BIN))));

                        if (magazineRecord == null) {
                            final WritePolicy writePolicy = new WritePolicy(storage.getAerospikeClient()
                                    .getWritePolicyDefault());
                            writePolicy.expiration = Constants.SHARDS_DEFAULT_TTL;
                            storage.getRetryerFactory()
                                    .getRetryer()
                                    .call(() -> {
                                        storage.getAerospikeClient()
                                                .put(writePolicy,
                                                        new Key(storage.getNamespace(),
                                                                storage.getMetaSetName(),
                                                                String.join(Constants.KEY_DELIMITER, magazineIdentifier,
                                                                        Constants.SHARDS_BIN)),
                                                        new Bin(Constants.SHARDS_BIN, storage.getShards()));
                                        return null;
                                    });
                            return true;
                        }

                        final int storedShards = magazineRecord.getInt(Constants.SHARDS_BIN);
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

                        return true;
                    }

                    @Override
                    public Boolean visitHBase() {
                        throw new UnsupportedOperationException();
                    }
                });
    }
}
