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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shard-count reconciliation: what an already-persisted shard count does to a differently
 * configured storage, and when promotion off a single shard is allowed.
 */
class MagazineShardingTest extends AerospikeMagazineTestBase {

    @Test
    public void persistedShardCountIsAdoptedWithoutOptIn() {
        String magazineIdentifier = "SHARD_ADOPT_MAGAZINE";
        String metaSet = "FARM_ID_META_SET";
        seedShardConfiguration(metaSet, magazineIdentifier, 2);

        // Storage is configured for 16 shards, but allowShardIncrease defaults to false, so the
        // persisted layout wins. Booting must never silently rewrite shared, persisted state.
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();

        assertEquals(2, magazine.getShards());
        assertEquals(2, readShardCount(metaSet, magazineIdentifier));
    }

    @Test
    public void shardIncreaseIsPersistedWhenExplicitlyAllowed() {
        String magazineIdentifier = "SHARD_INCREASE_MAGAZINE";
        String metaSet = "FARM_ID_META_SET";
        seedShardConfiguration(metaSet, magazineIdentifier, 2);

        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildStorage(
                        buildShardIncreaseConfig(16),
                        String.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient))
                .build();

        assertEquals(16, magazine.getShards());
        assertEquals(16, readShardCount(metaSet, magazineIdentifier));
    }

    @Test
    public void unshardedMagazineIsNotPromotedWithoutOptIn() {
        String magazineIdentifier = "SHARD_PROMOTE_MAGAZINE";
        String metaSet = "FARM_ID_META_SET";
        seedShardConfiguration(metaSet, magazineIdentifier, 1);

        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();

        assertEquals(1, magazine.getShards());
        assertEquals(1, readShardCount(metaSet, magazineIdentifier));
    }

    @Test
    public void drainedUnshardedMagazineIsPromotedWhenAllowed() {
        String magazineIdentifier = "SHARD_PROMOTE_DRAINED";
        String metaSet = "FARM_ID_META_SET";
        seedShardConfiguration(metaSet, magazineIdentifier, 1);

        Magazine<String> unsharded = Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();
        assertTrue(unsharded.load("DATA"));
        assertEquals("DATA", unsharded.fire().getData());

        Magazine<String> promoted = Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildStorage(
                        buildShardIncreaseConfig(16),
                        String.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient))
                .build();

        assertEquals(16, promoted.getShards());
        assertEquals(16, readShardCount(metaSet, magazineIdentifier));
        assertTrue(promoted.load("AFTER_PROMOTION"));
        assertEquals("AFTER_PROMOTION", promoted.fire().getData());
    }

    @Test
    public void unshardedMagazineWithUndeliveredRecordsIsNotPromoted() {
        String magazineIdentifier = "SHARD_PROMOTE_PENDING";
        String metaSet = "FARM_ID_META_SET";
        seedShardConfiguration(metaSet, magazineIdentifier, 1);

        Magazine<String> unsharded = Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();
        assertTrue(unsharded.load("STILL_QUEUED"));

        // Flat-key records are unreachable once the magazine turns sharded, so a promotion that
        // would strand them must fail rather than silently lose data.
        assertMagazineError(ErrorCode.INVALID_SHARDS, () -> Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildStorage(
                        buildShardIncreaseConfig(16),
                        String.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient))
                .build());
        assertEquals(1, readShardCount(metaSet, magazineIdentifier));
    }

    @Test
    public void oneStorageServesMagazinesWithDifferentShardCounts() {
        String metaSet = "FARM_ID_META_SET";
        seedShardConfiguration(metaSet, "MIXED_SHARDS_TWO", 2);
        seedShardConfiguration(metaSet, "MIXED_SHARDS_EIGHT", 8);
        BaseMagazineStorage<String> storage = buildMagazineStorage(String.class, false);

        Magazine<String> two = Magazine.<String>builder()
                .magazineIdentifier("MIXED_SHARDS_TWO").baseMagazineStorage(storage).build();
        Magazine<String> eight = Magazine.<String>builder()
                .magazineIdentifier("MIXED_SHARDS_EIGHT").baseMagazineStorage(storage).build();

        assertEquals(2, two.getShards());
        assertEquals(8, eight.getShards());
        assertEquals(2, two.getMetaData().size());
        assertEquals(8, eight.getMetaData().size());
        assertTrue(two.load("TWO"));
        assertTrue(eight.load("EIGHT"));
        assertEquals("TWO", two.fire().getData());
        assertEquals("EIGHT", eight.fire().getData());
    }

    private void seedShardConfiguration(final String metaSet,
                                        final String magazineIdentifier,
                                        final int shards) {
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_SHARDS"),
                new Bin(AerospikeConstants.SHARDS_BIN, shards),
                new Bin(AerospikeConstants.METADATA_SCHEMA_VERSION, AerospikeConstants.UNIFIED_METADATA_SCHEMA_VERSION));
    }

    private int readShardCount(final String metaSet, final String magazineIdentifier) {
        return aerospikeClient.get(
                        aerospikeClient.getReadPolicyDefault(),
                        new Key("NAMESPACE", metaSet, magazineIdentifier + "_SHARDS"))
                .getInt(AerospikeConstants.SHARDS_BIN);
    }

    private AerospikeStorageConfig buildShardIncreaseConfig(final int shards) {
        return AerospikeStorageConfig.builder()
                .namespace("NAMESPACE")
                .dataSetName("DATA_SET")
                .metaSetName("META_SET")
                .recordTtl(30 * 24 * 60 * 60)
                .metaDataTtl(2 * 30 * 24 * 60 * 60)
                .shards(shards)
                .allowShardIncrease(true)
                .build();
    }
}
