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

import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikeNaming;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.github.rholder.retry.RetryException;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.MagazineManager;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.server.AerospikeTestContainer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.testcontainers.containers.GenericContainer;

/**
 * @author shantanu.tiwari
 */
@SuppressWarnings("unchecked")
public class MagazineTest {

    private final Random random = ThreadLocalRandom.current();
    private MagazineManager magazineManager;
    private AerospikeClient aerospikeClient;
    private static GenericContainer<?> aerospikeContainer;

    @BeforeEach
    public void setup() throws Exception {

        aerospikeContainer = AerospikeTestContainer.initServerForTesting("NAMESPACE", 3000);

        ClientPolicy clientPolicy = new ClientPolicy();

        aerospikeClient = new AerospikeClient(clientPolicy, new Host("localhost", aerospikeContainer.getMappedPort(3000)));

        magazineManager = new MagazineManager("CLIENT_ID");
        magazineManager.refresh(List.of(Magazine.<String>builder()
                        .magazineIdentifier("MAGAZINE_ID1")
                        .baseMagazineStorage(buildMagazineStorage(String.class))
                        .build(),
                Magazine.<Long>builder()
                        .magazineIdentifier("MAGAZINE_ID2")
                        .baseMagazineStorage(buildMagazineStorage(Long.class))
                        .build(),
                Magazine.<Integer>builder()
                        .magazineIdentifier("MAGAZINE_ID3")
                        .baseMagazineStorage(buildMagazineStorage(Integer.class))
                        .build(),
                Magazine.<String>builder()
                        .magazineIdentifier("MAGAZINE_ID4")
                        .baseMagazineStorage(buildMagazineStorage(String.class))
                        .build(),
                Magazine.<String>builder()
                        .magazineIdentifier("MAGAZINE_ID5")
                        .baseMagazineStorage(buildMagazineStorage(String.class))
                        .build()));
    }

    @Test
    public void stringMagazineTest() {
        Magazine<String> magazine = magazineManager.getMagazine("MAGAZINE_ID5");
        Magazine<String> magazine2 = magazineManager.getMagazine("MAGAZINE_ID4");

        MetaData metaData = collectMetaData(magazine.getMetaData());
        assertEquals(0, metaData.getFireCounter());
        assertEquals(0, metaData.getLoadCounter());
        assertEquals(0, metaData.getFirePointer());
        assertEquals(0, metaData.getLoadPointer());

        boolean success = magazine.load("DATA1");
        assertTrue(success);
        magazine.load("DATA1");

        success = magazine2.load("DATA1");
        assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        assertEquals(0, metaData.getFireCounter());
        assertEquals(1, metaData.getLoadCounter());
        assertEquals(0, metaData.getFirePointer());
        assertEquals(1, metaData.getLoadPointer());

        MagazineData<String> data = magazine.fire();
        assertEquals(1, data.getFirePointer());
        assertEquals("DATA1", data.getData());
        magazine.delete(data);
        assertNull(aerospikeClient.get(aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "DATA_SET", dataKey(data, 16))));

        metaData = collectMetaData(magazine.getMetaData());
        assertEquals(1, metaData.getFireCounter());
        assertEquals(1, metaData.getLoadCounter());
        assertEquals(1, metaData.getFirePointer());
        assertEquals(1, metaData.getLoadPointer());

        success = magazine.reload("DATA1");
        assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        assertEquals(1, metaData.getLoadCounter());
        assertEquals(2, metaData.getLoadPointer());
    }

    @Test
    public void shardConfigurationHasFiveYearRetention() {
        Record shardConfiguration = aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_META_SET", "MAGAZINE_ID1_SHARDS"));

        assertNotNull(shardConfiguration);
        assertEquals(16, shardConfiguration.getInt(AerospikeConstants.SHARDS_BIN));
        assertEquals(AerospikeConstants.UNIFIED_METADATA_SCHEMA_VERSION,
                shardConfiguration.getInt(AerospikeConstants.METADATA_SCHEMA_VERSION));
        assertTrue(shardConfiguration.getLong(AerospikeConstants.CREATED_AT) > 0);
        assertTrue(shardConfiguration.getTimeToLive() <= AerospikeConstants.SHARD_CONFIGURATION_TTL_SECONDS);
        assertTrue(shardConfiguration.getTimeToLive() >= AerospikeConstants.SHARD_CONFIGURATION_TTL_SECONDS - 60);
    }

    @Test
    public void emptyMagazineReturnsNothingToFire() throws ExecutionException, RetryException {
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("EMPTY_CACHE_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();

        assertMagazineError(ErrorCode.NOTHING_TO_FIRE, magazine::fire);
    }

    @Test
    public void freshMagazineStoresCountersAndPointersTogether() throws ExecutionException, RetryException {
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
    public void activeShardDiscoveryRequiresPublishedCounter() throws ExecutionException, RetryException {
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
    public void versionlessMagazineContinuesUsingLegacyMetadata() throws ExecutionException, RetryException {
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
    public void storageCanServeLegacyAndUnifiedMagazines() throws ExecutionException, RetryException {
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
    public void persistedShardCountIsAdoptedWithoutOptIn() throws ExecutionException, RetryException {
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
    public void shardIncreaseIsPersistedWhenExplicitlyAllowed() throws ExecutionException, RetryException {
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
    public void unshardedMagazineIsNotPromotedWithoutOptIn() throws ExecutionException, RetryException {
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
    public void drainedUnshardedMagazineIsPromotedWhenAllowed() throws ExecutionException, RetryException {
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
    public void unshardedMagazineWithUndeliveredRecordsIsNotPromoted()
            throws ExecutionException, RetryException {
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
    public void oneStorageServesMagazinesWithDifferentShardCounts()
            throws ExecutionException, RetryException {
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

    @Test
    public void magazineCannotDeleteAnotherMagazinesData() throws ExecutionException, RetryException {
        Magazine<String> first = Magazine.<String>builder()
                .magazineIdentifier("DELETE_OWNER_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();
        Magazine<String> second = Magazine.<String>builder()
                .magazineIdentifier("DELETE_OTHER_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();
        assertTrue(second.load("DATA"));
        MagazineData<String> magazineData = second.fire();

        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> first.delete(magazineData));
    }

    @Test
    public void missingRecordReturnsNothingToFireAfterShardIsExhausted() throws ExecutionException, RetryException {
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("MISSING_RECORD_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();

        assertTrue(magazine.load("DATA"));
        deleteOnlyLoadedRecord(magazine);
        assertMagazineError(ErrorCode.NOTHING_TO_FIRE, magazine::fire);
    }

    @Test
    public void fireSkipsMissingPointerWithinActiveShard() throws ExecutionException, RetryException {
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("MAGAZINE_WITH_POINTER_HOLE")
                .baseMagazineStorage(buildStorage(
                        buildStorageConfig("NAMESPACE", "HOLE_DATA", "HOLE_META",
                                30 * 24 * 60 * 60, 2 * 30 * 24 * 60 * 60, 1),
                        String.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient))
                .build();

        assertTrue(magazine.load("MISSING"));
        assertTrue(magazine.load("DELIVERABLE"));
        assertTrue(aerospikeClient.delete(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_HOLE_DATA", "MAGAZINE_WITH_POINTER_HOLE_1")));

        assertEquals("DELIVERABLE", magazine.fire().getData());
        MetaData metaData = collectMetaData(magazine.getMetaData());
        assertEquals(2, metaData.getFirePointer());
        assertEquals(1, metaData.getFireCounter());
    }

    @Test
    public void concurrentFireClaimsEachRecordOnce() throws Exception {
        Magazine<String> magazine = buildUnshardedMagazine(
                "CONCURRENT_FIRE_MAGAZINE", "CONCURRENT_FIRE_DATA", "CONCURRENT_FIRE_META");
        int records = 20;
        for (int i = 0; i < records; i++) {
            assertTrue(magazine.load("DATA_" + i));
        }

        ExecutorService executor = Executors.newFixedThreadPool(8);
        long startedAt = System.nanoTime();
        try {
            List<Future<MagazineData<String>>> futures = new ArrayList<>(records);
            for (int i = 0; i < records; i++) {
                futures.add(executor.submit(magazine::fire));
            }

            Set<String> firedData = new HashSet<>();
            Set<Long> firedPointers = new HashSet<>();
            for (Future<MagazineData<String>> future : futures) {
                MagazineData<String> magazineData = future.get(30, TimeUnit.SECONDS);
                firedData.add(magazineData.getData());
                firedPointers.add(magazineData.getFirePointer());
            }

            assertEquals(records, firedData.size());
            assertEquals(records, firedPointers.size());
            MetaData metaData = collectMetaData(magazine.getMetaData());
            assertEquals(records, metaData.getFirePointer());
            assertEquals(records, metaData.getFireCounter());
            // Losing the pointer claim must back off, not spin. Without a wait strategy this
            // still passes but hammers the server; the bound keeps that regression visible.
            assertTrue(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startedAt) < 30);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void fireSkipsMoreThanFiveMissingPointers() throws ExecutionException, RetryException {
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("MAGAZINE_WITH_MANY_POINTER_HOLES")
                .baseMagazineStorage(buildStorage(
                        buildStorageConfig("NAMESPACE", "MANY_HOLES_DATA", "MANY_HOLES_META",
                                30 * 24 * 60 * 60, 2 * 30 * 24 * 60 * 60, 1),
                        String.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient))
                .build();

        for (int i = 0; i < 11; i++) {
            assertTrue(magazine.load("DATA_" + i));
        }
        for (int pointer = 1; pointer <= 10; pointer++) {
            assertTrue(aerospikeClient.delete(
                    aerospikeClient.getWritePolicyDefault(),
                    new Key("NAMESPACE", "FARM_ID_MANY_HOLES_DATA",
                            "MAGAZINE_WITH_MANY_POINTER_HOLES_" + pointer)));
        }

        assertEquals("DATA_10", magazine.fire().getData());
    }

    @Test
    public void fireGivesUpWithRetriesExhaustedWhenHoleBudgetIsSpent()
            throws ExecutionException, RetryException {
        // A run of holes longer than the budget must NOT report NOTHING_TO_FIRE: data may still
        // exist further along the shard, so the caller has to be able to tell "gave up" from
        // "queue is empty".
        AerospikeStorageConfig config = buildStorageConfig("NAMESPACE", "HOLE_BUDGET_DATA",
                "HOLE_BUDGET_META", 30 * 24 * 60 * 60, 2 * 30 * 24 * 60 * 60, 1);
        config.setMaxFireHoleSkips(3);
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("HOLE_BUDGET_MAGAZINE")
                .baseMagazineStorage(buildStorage(config, String.class, false,
                        "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient))
                .build();

        for (int i = 0; i < 6; i++) {
            assertTrue(magazine.load("DATA_" + i));
        }
        for (int pointer = 1; pointer <= 5; pointer++) {
            assertTrue(aerospikeClient.delete(
                    aerospikeClient.getWritePolicyDefault(),
                    new Key("NAMESPACE", "FARM_ID_HOLE_BUDGET_DATA", "HOLE_BUDGET_MAGAZINE_" + pointer)));
        }

        assertMagazineError(ErrorCode.RETRIES_EXHAUSTED, magazine::fire);
    }

    @Test
    public void fireSurfacesMissingMetadataRatherThanHidingTheShard()
            throws ExecutionException, RetryException {
        // metaDataTtl is validated to outlive recordTtl, so a missing metadata record is an
        // invariant violation, not a drained shard. It must be loud rather than silently
        // suppressing the shard, which would look identical to an empty queue.
        //
        // Reachable only once the active-shard cache is warm: cold, the cache loader sees no
        // metadata and correctly reports NOTHING_TO_FIRE. So fire once to warm it, then delete
        // the record underneath - which is precisely the race this guard exists for.
        Magazine<String> magazine = buildUnshardedMagazine(
                "MISSING_META_MAGAZINE", "MISSING_META_DATA", "MISSING_META_META");
        assertTrue(magazine.load("FIRST"));
        assertTrue(magazine.load("SECOND"));
        assertEquals("FIRST", magazine.fire().getData());

        assertTrue(aerospikeClient.delete(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_MISSING_META_META", "MISSING_META_MAGAZINE_METADATA")));

        assertMagazineError(ErrorCode.MAGAZINE_UNPREPARED, magazine::fire);
    }

    @Test
    public void dedupeSuppressesRepeatedLoadOverTheShippedDlmClasspath()
            throws ExecutionException, RetryException {
        // Guards the DLM classpath: magazine-core excludes every DLM transitive, on the basis that
        // the Aerospike lock path only needs aerospike-client, guava-retrying and slf4j, which we
        // already declare. If that ever stops holding, this fails with NoClassDefFoundError.
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("DEDUPE_CLASSPATH_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, true))
                .build();

        assertTrue(magazine.load("SAME"));
        assertTrue(magazine.load("SAME"));
        assertEquals(1, collectMetaData(magazine.getMetaData()).getLoadCounter());
        assertEquals("SAME", magazine.fire().getData());
    }

    @Test
    public void interruptedFireRetryPreservesInterruptStatus() throws ExecutionException, RetryException {
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("INTERRUPTED_FIRE_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();

        assertTrue(magazine.load("DATA"));
        deleteOnlyLoadedRecord(magazine);
        Thread.currentThread().interrupt();
        try {
            assertMagazineError(ErrorCode.RETRIES_EXHAUSTED, magazine::fire);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void dedupeDisabledDoesNotInitializeLockManager() throws ExecutionException, RetryException {
        AerospikeStorage<String> storage = buildMagazineStorage(String.class, false);
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("MAGAZINE_WITHOUT_DEDUPE")
                .baseMagazineStorage(storage)
                .build();

        assertFalse(storage.isEnableDeDupe());
        assertTrue(magazine.load("DATA"));
        assertTrue(magazine.load("DATA"));
        assertEquals(2, collectMetaData(magazine.getMetaData()).getLoadCounter());
    }

    @Test
    public void magazinePeekTest() {
        Magazine<String> magazine = magazineManager.getMagazine("MAGAZINE_ID1");
        magazine.load("DATA1");
        magazine.load("DATA2");
        magazine.load("DATA3");
        magazine.load("DATA4");
        magazine.load("DATA5");
        Map<String, MetaData> detailedMetaData = magazine.getMetaData();
        String shardId = detailedMetaData.keySet()
                .stream()
                .filter(shard -> detailedMetaData.get(shard)
                        .getLoadPointer() > 0)
                .findAny()
                .get();
        long randomPointerInShard = detailedMetaData.get(shardId)
                .getLoadPointer() > 1
                ? random.nextLong(1, detailedMetaData.get(shardId)
                .getLoadPointer())
                : 1;
        Map<String, MetaData> metadataBeforePeek = magazine.getMetaData();
        Map<Integer, Set<Long>> pointers = Map.of(
                Integer.parseInt(shardId.substring(shardId.indexOf(AerospikeConstants.KEY_DELIMITER) + 1)),
                Set.of(randomPointerInShard));
        Set<MagazineData<String>> magazineDataSet = magazine.peek(
                pointers);
        assertEquals(1, magazineDataSet.size());
        MagazineData<String> magazineData = magazineDataSet.iterator().next();
        assertNotNull(magazineData.getData());
        assertEquals("MAGAZINE_ID1", magazineData.getMagazineIdentifier());
        assertEquals(randomPointerInShard, magazineData.getFirePointer());
        assertEquals(Integer.valueOf(
                Integer.parseInt(shardId.substring(shardId.indexOf(AerospikeConstants.KEY_DELIMITER) + 1))),
                magazineData.getShard());
        assertEquals(magazineDataSet, magazine.peek(pointers));
        assertEquals(metadataBeforePeek, magazine.getMetaData());
    }

    @Test
    public void peekRejectsShardOutsideMagazineRange() {
        Magazine<String> magazine = magazineManager.getMagazine("MAGAZINE_ID1");
        magazine.load("DATA1");

        // 32 is beyond this magazine's 16 shards. Previously this silently produced a key that
        // could never match; it is a caller error and must surface as one.
        assertMagazineError(ErrorCode.INVALID_SHARDS,
                () -> magazine.peek(Map.of(32, Set.of(10L, 20L))));
    }

    @Test
    public void longMagazineTest() {
        Magazine<Long> magazine = magazineManager.getMagazine("MAGAZINE_ID2");

        MetaData metaData = collectMetaData(magazine.getMetaData());
        assertEquals(0, metaData.getFireCounter());
        assertEquals(0, metaData.getLoadCounter());
        assertEquals(0, metaData.getFirePointer());
        assertEquals(0, metaData.getLoadPointer());

        boolean success = magazine.load(12L);
        assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        assertEquals(0, metaData.getFireCounter());
        assertEquals(1, metaData.getLoadCounter());
        assertEquals(0, metaData.getFirePointer());
        assertEquals(1, metaData.getLoadPointer());

        MagazineData<Long> data = magazine.fire();
        assertEquals(12L, data.getData()
                .longValue());

        metaData = collectMetaData(magazine.getMetaData());
        assertEquals(1, metaData.getFireCounter());
        assertEquals(1, metaData.getLoadCounter());
        assertEquals(1, metaData.getFirePointer());
        assertEquals(1, metaData.getLoadPointer());

        success = magazine.reload(12L);
        assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        assertEquals(1, metaData.getLoadCounter());
        assertEquals(2, metaData.getLoadPointer());
    }

    @Test
    public void integerMagazineTest() {
        Magazine<Integer> magazine = magazineManager.getMagazine("MAGAZINE_ID3");

        MetaData metaData = collectMetaData(magazine.getMetaData());
        assertEquals(0, metaData.getFireCounter());
        assertEquals(0, metaData.getLoadCounter());
        assertEquals(0, metaData.getFirePointer());
        assertEquals(0, metaData.getLoadPointer());

        boolean success = magazine.load(12);
        assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        assertEquals(0, metaData.getFireCounter());
        assertEquals(1, metaData.getLoadCounter());
        assertEquals(0, metaData.getFirePointer());
        assertEquals(1, metaData.getLoadPointer());

        success = magazine.reload(12);
        assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        assertEquals(1, metaData.getLoadCounter());
        assertEquals(2, metaData.getLoadPointer());
    }

    @Test
    public void exceptionsTest() {
        MagazineException typeMismatch = assertThrows(MagazineException.class, () -> {
            Magazine<Integer> magazine = magazineManager.getMagazine("MAGAZINE_ID1");
            magazine.load(12);
        });
        assertEquals(ErrorCode.DATA_TYPE_MISMATCH, typeMismatch.getErrorCode());

        MagazineException notFound = assertThrows(MagazineException.class,
                () -> magazineManager.getMagazine("MAGAZINE1234"));
        assertEquals(ErrorCode.MAGAZINE_NOT_FOUND, notFound.getErrorCode());
    }

    @Test
    public void configurationValidationTest() {
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () ->
                Magazine.<String>builder()
                        .magazineIdentifier("MAGAZINE_ID")
                        .build());
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () ->
                Magazine.<String>builder()
                        .magazineIdentifier(" ")
                        .baseMagazineStorage(buildMagazineStorage(String.class, false))
                        .build());
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(null, String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig(null, "DATA_SET", "META_SET", 100, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", " ", "META_SET", 100, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", " ", 100, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 0, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 200, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_SHARDS, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 0), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), String.class,
                false, " ", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), String.class,
                false, "FARM_ID", null, MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", null, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, null));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), null,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
    }

    @Test
    public void payloadTypeValidationTest() throws ExecutionException, RetryException {
        Magazine<String> stringMagazine = magazineManager.getMagazine("MAGAZINE_ID1");
        assertMagazineError(ErrorCode.DATA_TYPE_MISMATCH, () -> stringMagazine.load(null));
        assertMagazineError(ErrorCode.DATA_TYPE_MISMATCH, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), Number.class,
                true, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));

        Magazine<Number> numberMagazine = Magazine.<Number>builder()
                .magazineIdentifier("NUMBER_MAGAZINE")
                .baseMagazineStorage(buildStorage(
                        buildStorageConfig("NAMESPACE", "NUMBER_DATA", "NUMBER_META", 100, 200, 1),
                        Number.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient))
                .build();
        assertTrue(numberMagazine.load(1));
    }

    @Test
    public void notImplementedGlobalScopeTest() throws ExecutionException, RetryException {
        MagazineException exception = assertThrows(MagazineException.class, () -> Magazine.<Long>builder()
                    .magazineIdentifier("MAGAZINE_ID")
                    .baseMagazineStorage(AerospikeStorage.<Long>builder()
                            .clazz(Long.class)
                            .storageConfig(AerospikeStorageConfig.builder()
                                    .dataSetName("DATA_SET")
                                    .metaSetName("META_SET")
                                    .namespace("NAMESPACE")
                                    .shards(16)
                                    .build())
                            .aerospikeClient(aerospikeClient)
                            .enableDeDupe(true)
                            .farmId("FARM_ID")
                            .clientId("CLIENT_ID")
                            .scope(MagazineScope.GLOBAL)
                            .build())
                    .build());
        assertEquals(ErrorCode.NOT_IMPLEMENTED, exception.getErrorCode());
    }

    /** Builds the data-record key through the production key layout, so a divergence would fail. */
    private String dataKey(final MagazineData<String> data, final int shards) {
        return AerospikeNaming.name(
                new MagazineContext(data.getMagazineIdentifier(),
                        AerospikeConstants.UNIFIED_METADATA_SCHEMA_VERSION, shards),
                data.getShard(), String.valueOf(data.getFirePointer()));
    }

    private <T> BaseMagazineStorage<T> buildMagazineStorage(Class<T> clazz) {
        return buildMagazineStorage(clazz, true);
    }

    private <T> AerospikeStorage<T> buildMagazineStorage(Class<T> clazz, boolean enableDeDupe) {
        return buildStorage(buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET",
                        30 * 24 * 60 * 60, 2 * 30 * 24 * 60 * 60, 16),
                clazz, enableDeDupe, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient);
    }

    private Magazine<String> buildUnshardedMagazine(final String magazineIdentifier,
            final String dataSetName,
            final String metaSetName) throws ExecutionException, RetryException {
        return Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildStorage(
                        buildStorageConfig("NAMESPACE", dataSetName, metaSetName,
                                30 * 24 * 60 * 60, 2 * 30 * 24 * 60 * 60, 1),
                        String.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient))
                .build();
    }

    private <T> AerospikeStorage<T> buildStorage(final AerospikeStorageConfig config,
            final Class<T> clazz,
            final boolean enableDeDupe,
            final String farmId,
            final String clientId,
            final MagazineScope scope,
            final AerospikeClient client) {
        return AerospikeStorage.<T>builder()
                .clazz(clazz)
                .storageConfig(config)
                .aerospikeClient(client)
                .enableDeDupe(enableDeDupe)
                .farmId(farmId)
                .clientId(clientId)
                .scope(scope)
                .build();
    }

    private AerospikeStorageConfig buildStorageConfig(final String namespace,
            final String dataSetName,
            final String metaSetName,
            final int recordTtl,
            final int metaDataTtl,
            final int shards) {
        return AerospikeStorageConfig.builder()
                .namespace(namespace)
                .dataSetName(dataSetName)
                .metaSetName(metaSetName)
                .recordTtl(recordTtl)
                .metaDataTtl(metaDataTtl)
                .shards(shards)
                .build();
    }

    private void deleteOnlyLoadedRecord(final Magazine<String> magazine) {
        Map.Entry<String, MetaData> loadedShard = magazine.getMetaData().entrySet().stream()
                .filter(entry -> entry.getValue().getLoadPointer() > 0)
                .findFirst()
                .orElseThrow();
        int shard = Integer.parseInt(loadedShard.getKey().substring(loadedShard.getKey().indexOf('_') + 1));
        String key = "%s_SHARD_%d_%d".formatted(
                magazine.getMagazineIdentifier(), shard, loadedShard.getValue().getLoadPointer());
        assertTrue(aerospikeClient.delete(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_DATA_SET", key)));
    }

    private void assertMagazineError(final ErrorCode errorCode, final Executable action) {
        MagazineException exception = assertThrows(MagazineException.class, action);
        assertEquals(errorCode, exception.getErrorCode());
    }

    public MetaData collectMetaData(Map<String, MetaData> metaDataMap) {
        return MetaData.builder()
                .loadPointer(metaDataMap.values()
                        .stream()
                        .mapToLong(MetaData::getLoadPointer)
                        .sum())
                .loadCounter(metaDataMap.values()
                        .stream()
                        .mapToLong(MetaData::getLoadCounter)
                        .sum())
                .firePointer(metaDataMap.values()
                        .stream()
                        .mapToLong(MetaData::getFirePointer)
                        .sum())
                .fireCounter(metaDataMap.values()
                        .stream()
                        .mapToLong(MetaData::getFireCounter)
                        .sum())
                .build();
    }

}
