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
import com.aerospike.client.Record;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The persisted metadata schema: unified versus legacy split ledgers, and what happens when a
 * magazine written by an older version is opened by this one.
 */
class MagazineMetadataSchemaTest extends AerospikeMagazineTestBase {

    @Test
    public void shardConfigurationHasFiveYearRetention() {
        // Initialised here rather than relied on from a shared fixture: this used to assert against
        // a magazine some other test's setup happened to have created.
        buildMagazineStorage(String.class).initialize("SHARD_CONFIG_MAGAZINE");

        Record shardConfiguration = aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_META_SET", "SHARD_CONFIG_MAGAZINE_SHARDS"));

        assertNotNull(shardConfiguration);
        assertEquals(16, shardConfiguration.getInt(AerospikeConstants.SHARDS_BIN));
        assertEquals(AerospikeConstants.UNIFIED_METADATA_SCHEMA_VERSION,
                shardConfiguration.getInt(AerospikeConstants.METADATA_SCHEMA_VERSION));
        assertTrue(shardConfiguration.getLong(AerospikeConstants.CREATED_AT) > 0);
        assertTrue(shardConfiguration.getTimeToLive() <= AerospikeConstants.SHARD_CONFIGURATION_TTL_SECONDS);
        assertTrue(shardConfiguration.getTimeToLive() >= AerospikeConstants.SHARD_CONFIGURATION_TTL_SECONDS - 60);
    }

    @Test
    public void freshMagazineStoresCountersAndPointersTogether() {
        Magazine<String> magazine = buildUnshardedMagazine(
                "UNIFIED_METADATA_MAGAZINE", "UNIFIED_METADATA_DATA", "UNIFIED_METADATA_META");

        assertTrue(magazine.load("DATA"));

        Record metadata = aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_UNIFIED_METADATA_META", "UNIFIED_METADATA_MAGAZINE_METADATA"));
        assertNotNull(metadata);
        assertEquals(1, metadata.getLong(AerospikeConstants.LOAD_POINTER));
        assertEquals(1, metadata.getLong(AerospikeConstants.LOAD_COUNTER));
        assertEquals(0, metadata.getLong(AerospikeConstants.FIRE_POINTER));
        assertEquals(0, metadata.getLong(AerospikeConstants.FIRE_COUNTER));
        assertNull(aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_UNIFIED_METADATA_META", "UNIFIED_METADATA_MAGAZINE_POINTERS")));
        assertNull(aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_UNIFIED_METADATA_META", "UNIFIED_METADATA_MAGAZINE_COUNTERS")));
    }

    @Test
    public void activeShardDiscoveryRequiresPublishedCounter() {
        Magazine<String> magazine = buildUnshardedMagazine(
                "COUNTER_DRIFT_MAGAZINE", "COUNTER_DRIFT_DATA", "COUNTER_DRIFT_META");

        assertTrue(magazine.load("DATA"));
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_COUNTER_DRIFT_META", "COUNTER_DRIFT_MAGAZINE_METADATA"),
                new Bin(AerospikeConstants.LOAD_COUNTER, 0L),
                new Bin(AerospikeConstants.FIRE_COUNTER, 10L));

        MetaData metaData = collectMetaData(magazine.getMetaData());
        assertEquals(0, metaData.getLoadCounter());
        assertEquals(10, metaData.getFireCounter());
        assertEquals(1, metaData.getLoadPointer());
        assertMagazineError(ErrorCode.NOTHING_TO_FIRE, magazine::fire);
    }

    @Test
    public void versionlessMagazineContinuesUsingLegacyMetadata() {
        String magazineIdentifier = "LEGACY_METADATA_MAGAZINE";
        String metaSet = "FARM_ID_LEGACY_METADATA_META";
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_SHARDS"),
                new Bin(AerospikeConstants.SHARDS_BIN, 1));
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_POINTERS"),
                new Bin(AerospikeConstants.LOAD_POINTER, 0L),
                new Bin(AerospikeConstants.FIRE_POINTER, 0L));
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_COUNTERS"),
                new Bin(AerospikeConstants.LOAD_COUNTER, 0L),
                new Bin(AerospikeConstants.FIRE_COUNTER, 0L));

        Magazine<String> magazine = buildUnshardedMagazine(
                magazineIdentifier, "LEGACY_METADATA_DATA", "LEGACY_METADATA_META");
        assertTrue(magazine.load("DATA"));

        MetaData metaData = collectMetaData(magazine.getMetaData());
        assertEquals(1, metaData.getLoadPointer());
        assertEquals(0, metaData.getFirePointer());
        assertEquals(1, metaData.getLoadCounter());
        assertEquals(0, metaData.getFireCounter());
        assertEquals("DATA", magazine.fire().getData());

        Record shardConfiguration = aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_SHARDS"));
        assertEquals(AerospikeConstants.LEGACY_METADATA_SCHEMA_VERSION,
                shardConfiguration.getInt(AerospikeConstants.METADATA_SCHEMA_VERSION));
        assertNull(aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_METADATA")));
    }

    @Test
    public void storageCanServeLegacyAndUnifiedMagazines() {
        AerospikeStorage<String> storage = buildMagazineStorage(String.class, false);
        String legacyIdentifier = "SHARED_STORAGE_LEGACY";
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_META_SET", legacyIdentifier + "_SHARDS"),
                new Bin(AerospikeConstants.SHARDS_BIN, 16));
        Magazine<String> legacy = Magazine.<String>builder()
                .magazineIdentifier(legacyIdentifier)
                .baseMagazineStorage(storage)
                .build();
        Magazine<String> unified = Magazine.<String>builder()
                .magazineIdentifier("SHARED_STORAGE_UNIFIED")
                .baseMagazineStorage(storage)
                .build();

        assertTrue(legacy.load("LEGACY"));
        assertTrue(unified.load("UNIFIED"));
        assertEquals("LEGACY", legacy.fire().getData());
        assertEquals("UNIFIED", unified.fire().getData());
    }

    @Test
    public void unsupportedMetadataSchemaIsRejected() {
        String magazineIdentifier = "UNSUPPORTED_SCHEMA_MAGAZINE";
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_META_SET", magazineIdentifier + "_SHARDS"),
                new Bin(AerospikeConstants.SHARDS_BIN, 16),
                new Bin(AerospikeConstants.METADATA_SCHEMA_VERSION, 99));

        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build());
    }
}
