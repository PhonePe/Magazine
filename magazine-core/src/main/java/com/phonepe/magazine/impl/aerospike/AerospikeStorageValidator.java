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

import com.aerospike.client.IAerospikeClient;
import com.phonepe.magazine.exception.MagazineExceptions;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Construction-time validation for {@link AerospikeStorage}, kept out of the storage itself so the
 * storage reads as orchestration.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class AerospikeStorageValidator {

    static AerospikeStorageConfig validateConfig(final AerospikeStorageConfig storageConfig) {
        if (Objects.isNull(storageConfig)) {
            throw MagazineExceptions.invalidConfiguration("Aerospike storage configuration is required.");
        }
        requireText(storageConfig.getNamespace(), "Aerospike namespace is required.");
        requireText(storageConfig.getDataSetName(), "Aerospike data set name is required.");
        requireText(storageConfig.getMetaSetName(), "Aerospike metadata set name is required.");
        if (storageConfig.getShards() < 1) {
            throw MagazineExceptions.invalidShards("Shard count must be at least 1.");
        }
        if (storageConfig.getMaxFireHoleSkips() < 1) {
            throw MagazineExceptions.invalidConfiguration("Max fire hole skips must be at least 1.");
        }
        if (storageConfig.getActiveShardRefreshSeconds() < 1) {
            throw MagazineExceptions.invalidConfiguration(
                    "Active shard refresh seconds must be at least 1.");
        }
        return storageConfig;
    }

    static void validateStorage(final IAerospikeClient aerospikeClient,
            final Class<?> clazz,
            final boolean enableDeDupe) {
        if (Objects.isNull(aerospikeClient)) {
            throw MagazineExceptions.invalidConfiguration("Aerospike client is required.");
        }
        if (Objects.isNull(clazz)) {
            throw MagazineExceptions.invalidConfiguration("Magazine data type is required.");
        }
        // Deduplication keys the marker record on the payload's toString(), which is only stable
        // and collision-free for these value types.
        if (enableDeDupe && !AerospikeConstants.DEDUPABLE_CLASSES.contains(clazz)) {
            throw MagazineExceptions.dataTypeMismatch(
                    "Deduplication is supported only for String, Long, and Integer data types.");
        }
    }

    private static void requireText(final String value, final String message) {
        if (Objects.isNull(value) || value.isBlank()) {
            throw MagazineExceptions.invalidConfiguration(message);
        }
    }
}
