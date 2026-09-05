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

package com.phonepe.magazine.entity;

import com.phonepe.magazine.exception.MagazineExceptions;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Immutable, per-magazine resolved configuration produced by
 * {@link com.phonepe.magazine.core.BaseMagazineStorage#initialize(String)} and passed to every
 * subsequent storage operation.
 * <p>
 * Shard count is a property of the <em>magazine</em>, not of the storage: a single storage
 * instance may serve magazines with differing shard counts, so it must never assume its own
 * configured shard count applies to a given magazine.
 */
@EqualsAndHashCode
@Getter
public final class MagazineContext {

    /**
     * Shard label format. Part of the public contract: these are the keys of the map returned by
     * {@link com.phonepe.magazine.Magazine#getMetaData()}.
     */
    private static final String SHARD_ID_FORMAT = "SHARD_%d";

    private final String magazineIdentifier;
    private final int storageSchemaVersion;
    private final int shards;

    /**
     * Precomputed {@code SHARD_<n>} key fragments, indexed by shard number. Derived purely from
     * {@link #shards}, so it is excluded from equality - this type is used as a cache key and
     * array equality would be both wasteful and redundant.
     */
    @EqualsAndHashCode.Exclude
    private final String[] shardIds;

    public MagazineContext(final String magazineIdentifier,
            final int storageSchemaVersion,
            final int shards) {
        if (Objects.isNull(magazineIdentifier) || magazineIdentifier.isBlank()) {
            throw MagazineExceptions.invalidConfiguration("Magazine identifier is required.");
        }
        if (shards < 1) {
            throw MagazineExceptions.invalidShards("Shard count must be at least 1.");
        }
        this.magazineIdentifier = magazineIdentifier;
        this.storageSchemaVersion = storageSchemaVersion;
        this.shards = shards;
        this.shardIds = createShardIds(shards);
    }

    /**
     * @return true when this magazine is unsharded, in which case keys carry no shard fragment.
     */
    public boolean isUnsharded() {
        return shards <= 1;
    }

    /**
     * @param shard shard number, or null for an unsharded magazine.
     * @return the {@code SHARD_<n>} key fragment for the given shard.
     */
    public String shardId(final Integer shard) {
        final int index = Objects.isNull(shard) ? 0 : shard;
        if (index < 0 || index >= shardIds.length) {
            throw MagazineExceptions.invalidShards(
                    String.format("Shard %d is out of range for magazine %s with %d shards.",
                            index, magazineIdentifier, shards));
        }
        return shardIds[index];
    }

    private static String[] createShardIds(final int shards) {
        final String[] ids = new String[shards];
        for (int shard = 0; shard < shards; shard++) {
            ids[shard] = SHARD_ID_FORMAT.formatted(shard);
        }
        return ids;
    }

}
