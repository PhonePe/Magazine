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

import com.phonepe.magazine.common.Constants;
import com.phonepe.magazine.common.MagazineData;
import com.phonepe.magazine.common.MetaData;
import com.phonepe.magazine.scope.MagazineScope;
import com.phonepe.magazine.util.CommonUtils;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public abstract class BaseMagazineStorage<T> {

    private final StorageType type;
    private final int recordTtl;
    private final int metaDataTtl;
    private final boolean enableDeDupe;
    private final int shards;
    private final String farmId;
    private final String clientId;
    private final MagazineScope scope;

    public BaseMagazineStorage(
            final StorageType type,
            final int recordTtl,
            final int metaDataTtl,
            final String farmId,
            final boolean enableDeDupe,
            final int shards,
            final String clientId,
            final MagazineScope scope) {
        this.type = type;
        this.recordTtl = recordTtl;
        this.metaDataTtl = metaDataTtl;
        this.enableDeDupe = enableDeDupe;
        this.farmId = farmId;
        this.shards = shards < 1
                ? Constants.MIN_SHARDS
                : shards;
        this.clientId = clientId;
        this.scope = scope;

        CommonUtils.validateMagazineScope(scope);
    }

    /**
     * Load data into the specified magazine.
     *
     * @param magazineIdentifier The identifier of the magazine to load data into.
     * @param data The data to be loaded.
     * @return True if the data was successfully loaded, false otherwise.
     */
    public abstract boolean load(
            final String magazineIdentifier,
            final T data
    );

    /**
     * Reload data into the specified magazine. This won't increase the load counter as the data was already loaded,
     * but load pointer will be incremented as the data will appended at the end.
     *
     * @param magazineIdentifier The identifier of the magazine to reload data into.
     * @param data The data to be reloaded.
     * @return True if the data was successfully reloaded, false otherwise.
     */
    public abstract boolean reload(
            final String magazineIdentifier,
            final T data
    );

    /**
     * Fire and retrieve data from the specified magazine.
     *
     * @param magazineIdentifier The identifier of the magazine to retrieve data from.
     * @return The MagazineData containing the fired data.
     */
    public abstract MagazineData<T> fire(final String magazineIdentifier);

    /**
     * Retrieve metadata of the specified magazine i.e the number of loaded or fired, pointers and counters.
     *
     * @param magazineIdentifier The identifier of the magazine to get metadata from.
     * @return A map containing metadata information.
     */
    public abstract Map<String, MetaData> getMetaData(final String magazineIdentifier);

    /**
     * Delete the provided MagazineData from the magazine.
     *
     * @param magazineData The MagazineData to be deleted.
     */
    public abstract void delete(final MagazineData<T> magazineData);

    /**
     * Peek data from specific shards and pointers within the magazine.
     *
     * @param magazineIdentifier The identifier of the magazine to peek from.
     * @param shardPointersMap A map where keys are shard identifiers and values are sets of pointers to peek from.
     * @return A set of MagazineData containing the peeked data.
     */
    public abstract Set<MagazineData<T>> peek(
            final String magazineIdentifier,
            final Map<Integer, Set<Long>> shardPointersMap
    );
}
