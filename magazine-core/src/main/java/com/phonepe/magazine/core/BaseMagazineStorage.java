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

package com.phonepe.magazine.core;

import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.entity.StorageType;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineExceptions;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Service provider interface for Magazine storage backends.
 * <p>
 * Implementations own persistence, sharding layout and metadata schema. A single storage instance
 * may serve many magazines, so no per-magazine state may be held on the storage itself - it is
 * resolved once by {@link #initialize(String)} into a {@link MagazineContext} which the caller
 * threads through every subsequent operation.
 */
@Getter
@EqualsAndHashCode
public abstract class BaseMagazineStorage<T> {

    private final StorageType type;
    private final int recordTtl;
    private final int metaDataTtl;
    private final boolean enableDeDupe;
    private final String farmId;
    private final String clientId;
    private final MagazineScope scope;

    protected BaseMagazineStorage(
            final StorageType type,
            final int recordTtl,
            final int metaDataTtl,
            final String farmId,
            final boolean enableDeDupe,
            final String clientId,
            final MagazineScope scope) {
        validate(type, recordTtl, metaDataTtl, farmId, clientId, scope);
        this.type = type;
        this.recordTtl = recordTtl;
        this.metaDataTtl = metaDataTtl;
        this.enableDeDupe = enableDeDupe;
        this.farmId = farmId;
        this.clientId = clientId;
        this.scope = scope;
    }

    /**
     * Resolve, and create if absent, the persisted configuration for a magazine.
     * <p>
     * Called exactly once per {@link com.phonepe.magazine.Magazine} instance at construction time.
     * Implementations must reconcile the requested configuration with what is already persisted -
     * in particular the shard count and metadata schema version - and fail loudly on any
     * incompatible change rather than silently rewriting persisted state.
     *
     * @param magazineIdentifier The magazine to resolve.
     * @return The resolved context for all subsequent operations on this magazine.
     */
    public abstract MagazineContext initialize(final String magazineIdentifier);

    /**
     * Load data into the specified magazine.
     *
     * @param context The magazine operation context.
     * @param data The data to be loaded.
     * @return True if the data was successfully loaded, false otherwise.
     */
    public abstract boolean load(
            final MagazineContext context,
            final T data
    );

    /**
     * Reload data into the specified magazine. This won't increase the load counter as the data was already loaded,
     * but load pointer will be incremented as the data will appended at the end.
     *
     * @param context The magazine operation context.
     * @param data The data to be reloaded.
     * @return True if the data was successfully reloaded, false otherwise.
     */
    public abstract boolean reload(
            final MagazineContext context,
            final T data
    );

    /**
     * Fire and retrieve data from the specified magazine.
     *
     * @param context The magazine operation context.
     * @return The MagazineData containing the fired data.
     */
    public abstract MagazineData<T> fire(final MagazineContext context);

    /**
     * Retrieve metadata of the specified magazine i.e the number of loaded or fired, pointers and counters.
     *
     * @param context The magazine operation context.
     * @return A map containing metadata information.
     */
    public abstract Map<String, MetaData> getMetaData(final MagazineContext context);

    /**
     * Delete the provided MagazineData from the magazine.
     *
     * @param context The magazine operation context.
     * @param magazineData The MagazineData to be deleted.
     */
    public abstract void delete(final MagazineContext context, final MagazineData<T> magazineData);

    /**
     * Peek data from specific shards and pointers within the magazine.
     *
     * @param context The magazine operation context.
     * @param shardPointersMap A map where keys are shard identifiers and values are sets of pointers to peek from.
     * @return A set of MagazineData containing the peeked data.
     */
    public abstract Set<MagazineData<T>> peek(
            final MagazineContext context,
            final Map<Integer, Set<Long>> shardPointersMap
    );

    private static void validate(final StorageType type,
            final int recordTtl,
            final int metaDataTtl,
            final String farmId,
            final String clientId,
            final MagazineScope scope) {
        if (Objects.isNull(type)) {
            throw MagazineExceptions.invalidConfiguration("Storage type is required.");
        }
        if (recordTtl <= 0) {
            throw MagazineExceptions.invalidConfiguration("Record TTL must be positive.");
        }
        // Metadata must outlive data: fire() reads the metadata record to locate the next data
        // record, so metadata expiring first would make live data unreachable and indistinguishable
        // from an uninitialised magazine.
        if (metaDataTtl <= recordTtl) {
            throw MagazineExceptions.invalidConfiguration("Metadata TTL must be greater than record TTL.");
        }
        if (Objects.isNull(farmId) || farmId.isBlank()) {
            throw MagazineExceptions.invalidConfiguration("Farm ID is required.");
        }
        if (Objects.isNull(clientId) || clientId.isBlank()) {
            throw MagazineExceptions.invalidConfiguration("Client ID is required.");
        }
        if (Objects.isNull(scope)) {
            throw MagazineExceptions.invalidConfiguration("Magazine scope is required.");
        }
        if (scope == MagazineScope.GLOBAL) {
            throw MagazineExceptions.of(ErrorCode.NOT_IMPLEMENTED, "Global scope is not implemented.");
        }
    }

}
