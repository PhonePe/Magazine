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

import com.github.rholder.retry.RetryException;
import com.phonepe.magazine.common.MagazineData;
import com.phonepe.magazine.common.MetaData;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.core.StorageTypeVisitor;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.impl.aerospike.AerospikeStorage;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.Builder;
import lombok.Data;
import lombok.AccessLevel;
import lombok.Getter;

@Data
public class Magazine<T> {

    private final BaseMagazineStorage<T> baseMagazineStorage;
    private final String magazineIdentifier;
    @Getter(AccessLevel.NONE)
    private final MagazineContext context;

    @Builder
    public Magazine(final BaseMagazineStorage<T> baseMagazineStorage,
            final String magazineIdentifier) throws ExecutionException, RetryException {
        if (Objects.isNull(baseMagazineStorage)) {
            throw invalidConfiguration("Magazine storage is required.");
        }
        if (Objects.isNull(magazineIdentifier) || magazineIdentifier.isBlank()) {
            throw invalidConfiguration("Magazine identifier is required.");
        }
        this.magazineIdentifier = magazineIdentifier;
        this.baseMagazineStorage = baseMagazineStorage;
        this.context = new MagazineContext(magazineIdentifier, validateStorage(baseMagazineStorage));
    }

    /**
     * Load data into the specified magazine.
     *
     * @param data The data to be loaded.
     * @return True if the data was successfully loaded, false otherwise.
     */
    public boolean load(final T data) {
        return baseMagazineStorage.load(context, data);
    }

    /**
     * Reload data into the specified magazine. This won't increase the load counter as the data was already loaded,
     * but load pointer will be incremented as the data will appended at the end.
     *
     * @param data The data to be reloaded.
     * @return True if the data was successfully reloaded, false otherwise.
     */
    public boolean reload(final T data) {
        return baseMagazineStorage.reload(context, data);
    }

    /**
     * Fire and retrieve data from the specified magazine.
     *
     * @return The MagazineData containing the fired data.
     */
    public MagazineData<T> fire() {
        return baseMagazineStorage.fire(context);
    }

    /**
     * Delete the provided MagazineData from the magazine.
     *
     * @param magazineData The MagazineData to be deleted.
     */
    public void delete(final MagazineData<T> magazineData) {
        baseMagazineStorage.delete(context, magazineData);
    }

    /**
     * Retrieve metadata of the specified magazine i.e the number of loaded or fired, pointers and counters.
     *
     * @return A map containing metadata information.
     */
    public Map<String, MetaData> getMetaData() {
        return baseMagazineStorage.getMetaData(context);
    }

    /**
     * Peek data from specific shards and pointers within the magazine.
     *
     * @param shardPointersMap A map where keys are shard identifiers and values are sets of pointers to peek from.
     * @return A set of MagazineData containing the peeked data.
     */
    public Set<MagazineData<T>> peek(final Map<Integer, Set<Long>> shardPointersMap) {
        return baseMagazineStorage.peek(context, shardPointersMap);
    }

    @SuppressWarnings("unchecked")
    private int validateStorage(final BaseMagazineStorage<T> baseMagazineStorage)
            throws ExecutionException, RetryException {
        return baseMagazineStorage.getType()
                .accept(new StorageTypeVisitor<Integer>() {
                    @Override
                    public Integer visitAerospike() throws ExecutionException, RetryException {
                        final AerospikeStorage<T> storage = (AerospikeStorage) baseMagazineStorage;
                        return AerospikeMagazineInitializer.initialize(storage, magazineIdentifier);
                    }
                });
    }

    private static MagazineException invalidConfiguration(final String message) {
        return MagazineException.builder()
                .errorCode(ErrorCode.INVALID_CONFIGURATION)
                .message(message)
                .build();
    }
}
