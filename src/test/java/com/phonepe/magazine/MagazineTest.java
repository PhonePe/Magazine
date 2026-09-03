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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.github.rholder.retry.RetryException;
import com.phonepe.magazine.common.Constants;
import com.phonepe.magazine.common.MagazineData;
import com.phonepe.magazine.common.MetaData;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.impl.aerospike.AerospikeStorage;
import com.phonepe.magazine.impl.aerospike.AerospikeStorageConfig;
import com.phonepe.magazine.scope.MagazineScope;
import com.phonepe.magazine.server.AerospikeTestContainer;
import io.appform.testcontainers.aerospike.AerospikeContainerConfiguration;
import io.appform.testcontainers.aerospike.AerospikeWaitStrategy;

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

import org.junit.*;
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

    @Before
    public void setup() throws Exception {

        AerospikeContainerConfiguration config = new AerospikeContainerConfiguration();
        config.setNamespace("NAMESPACE");
        config.setPort(3000);

        AerospikeWaitStrategy waitStrategy = new AerospikeWaitStrategy(config);

        aerospikeContainer = AerospikeTestContainer.initServerForTesting(config, waitStrategy);

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
        Assert.assertEquals(0, metaData.getFireCounter());
        Assert.assertEquals(0, metaData.getLoadCounter());
        Assert.assertEquals(0, metaData.getFirePointer());
        Assert.assertEquals(0, metaData.getLoadPointer());

        boolean success = magazine.load("DATA1");
        Assert.assertTrue(success);
        magazine.load("DATA1");

        success = magazine2.load("DATA1");
        Assert.assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(0, metaData.getFireCounter());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(0, metaData.getFirePointer());
        Assert.assertEquals(1, metaData.getLoadPointer());

        MagazineData<String> data = magazine.fire();
        Assert.assertEquals(1, data.getFirePointer());
        Assert.assertEquals("DATA1", data.getData());
        magazine.delete(data);
        Assert.assertNull(aerospikeClient.get(aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "DATA_SET", data.createAerospikeKey())));

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(1, metaData.getFireCounter());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(1, metaData.getFirePointer());
        Assert.assertEquals(1, metaData.getLoadPointer());

        success = magazine.reload("DATA1");
        Assert.assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(2, metaData.getLoadPointer());
    }

    @Test
    public void shardConfigurationHasFiveYearRetention() {
        Record shardConfiguration = aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_META_SET", "MAGAZINE_ID1_SHARDS"));

        Assert.assertNotNull(shardConfiguration);
        Assert.assertEquals(16, shardConfiguration.getInt(Constants.SHARDS_BIN));
        Assert.assertEquals(Constants.UNIFIED_METADATA_SCHEMA_VERSION,
                shardConfiguration.getInt(Constants.METADATA_SCHEMA_VERSION));
        Assert.assertTrue(shardConfiguration.getLong(Constants.CREATED_AT) > 0);
        Assert.assertTrue(shardConfiguration.getTimeToLive() <= Constants.SHARD_CONFIGURATION_TTL_SECONDS);
        Assert.assertTrue(shardConfiguration.getTimeToLive() >= Constants.SHARD_CONFIGURATION_TTL_SECONDS - 60);
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

        Assert.assertTrue(magazine.load("DATA"));

        Record metadata = aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_UNIFIED_METADATA_META", "UNIFIED_METADATA_MAGAZINE_METADATA"));
        Assert.assertNotNull(metadata);
        Assert.assertEquals(1, metadata.getLong(Constants.LOAD_POINTER));
        Assert.assertEquals(1, metadata.getLong(Constants.LOAD_COUNTER));
        Assert.assertEquals(0, metadata.getLong(Constants.FIRE_POINTER));
        Assert.assertEquals(0, metadata.getLong(Constants.FIRE_COUNTER));
        Assert.assertNull(aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_UNIFIED_METADATA_META", "UNIFIED_METADATA_MAGAZINE_POINTERS")));
        Assert.assertNull(aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_UNIFIED_METADATA_META", "UNIFIED_METADATA_MAGAZINE_COUNTERS")));
    }

    @Test
    public void activeShardDiscoveryRequiresPublishedCounter() throws ExecutionException, RetryException {
        Magazine<String> magazine = buildUnshardedMagazine(
                "COUNTER_DRIFT_MAGAZINE", "COUNTER_DRIFT_DATA", "COUNTER_DRIFT_META");

        Assert.assertTrue(magazine.load("DATA"));
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_COUNTER_DRIFT_META", "COUNTER_DRIFT_MAGAZINE_METADATA"),
                new Bin(Constants.LOAD_COUNTER, 0L),
                new Bin(Constants.FIRE_COUNTER, 10L));

        MetaData metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(0, metaData.getLoadCounter());
        Assert.assertEquals(10, metaData.getFireCounter());
        Assert.assertEquals(1, metaData.getLoadPointer());
        assertMagazineError(ErrorCode.NOTHING_TO_FIRE, magazine::fire);
    }

    @Test
    public void versionlessMagazineContinuesUsingLegacyMetadata() throws ExecutionException, RetryException {
        String magazineIdentifier = "LEGACY_METADATA_MAGAZINE";
        String metaSet = "FARM_ID_LEGACY_METADATA_META";
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_SHARDS"),
                new Bin(Constants.SHARDS_BIN, 1));
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_POINTERS"),
                new Bin(Constants.LOAD_POINTER, 0L),
                new Bin(Constants.FIRE_POINTER, 0L));
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_COUNTERS"),
                new Bin(Constants.LOAD_COUNTER, 0L),
                new Bin(Constants.FIRE_COUNTER, 0L));

        Magazine<String> magazine = buildUnshardedMagazine(
                magazineIdentifier, "LEGACY_METADATA_DATA", "LEGACY_METADATA_META");
        Assert.assertTrue(magazine.load("DATA"));

        MetaData metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(1, metaData.getLoadPointer());
        Assert.assertEquals(0, metaData.getFirePointer());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(0, metaData.getFireCounter());
        Assert.assertEquals("DATA", magazine.fire().getData());

        Record shardConfiguration = aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_SHARDS"));
        Assert.assertEquals(Constants.LEGACY_METADATA_SCHEMA_VERSION,
                shardConfiguration.getInt(Constants.METADATA_SCHEMA_VERSION));
        Assert.assertNull(aerospikeClient.get(
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
                new Bin(Constants.SHARDS_BIN, 16));
        Magazine<String> legacy = Magazine.<String>builder()
                .magazineIdentifier(legacyIdentifier)
                .baseMagazineStorage(storage)
                .build();
        Magazine<String> unified = Magazine.<String>builder()
                .magazineIdentifier("SHARED_STORAGE_UNIFIED")
                .baseMagazineStorage(storage)
                .build();

        Assert.assertTrue(legacy.load("LEGACY"));
        Assert.assertTrue(unified.load("UNIFIED"));
        Assert.assertEquals("LEGACY", legacy.fire().getData());
        Assert.assertEquals("UNIFIED", unified.fire().getData());
    }

    @Test
    public void shardIncreaseIsPersisted() throws ExecutionException, RetryException {
        String magazineIdentifier = "SHARD_INCREASE_MAGAZINE";
        String metaSet = "FARM_ID_META_SET";
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_SHARDS"),
                new Bin(Constants.SHARDS_BIN, 2),
                new Bin(Constants.METADATA_SCHEMA_VERSION, Constants.UNIFIED_METADATA_SCHEMA_VERSION));

        Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();

        Record shardConfiguration = aerospikeClient.get(
                aerospikeClient.getReadPolicyDefault(),
                new Key("NAMESPACE", metaSet, magazineIdentifier + "_SHARDS"));
        Assert.assertEquals(16, shardConfiguration.getInt(Constants.SHARDS_BIN));
    }

    @Test
    public void unsupportedMetadataSchemaIsRejected() {
        String magazineIdentifier = "UNSUPPORTED_SCHEMA_MAGAZINE";
        aerospikeClient.put(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_META_SET", magazineIdentifier + "_SHARDS"),
                new Bin(Constants.SHARDS_BIN, 16),
                new Bin(Constants.METADATA_SCHEMA_VERSION, 99));

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
        Assert.assertTrue(second.load("DATA"));
        MagazineData<String> magazineData = second.fire();

        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> first.delete(magazineData));
    }

    @Test
    public void missingRecordReturnsNothingToFireAfterShardIsExhausted() throws ExecutionException, RetryException {
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("MISSING_RECORD_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();

        Assert.assertTrue(magazine.load("DATA"));
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

        Assert.assertTrue(magazine.load("MISSING"));
        Assert.assertTrue(magazine.load("DELIVERABLE"));
        Assert.assertTrue(aerospikeClient.delete(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_HOLE_DATA", "MAGAZINE_WITH_POINTER_HOLE_1")));

        Assert.assertEquals("DELIVERABLE", magazine.fire().getData());
        MetaData metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(2, metaData.getFirePointer());
        Assert.assertEquals(1, metaData.getFireCounter());
    }

    @Test
    public void concurrentFireClaimsEachRecordOnce() throws Exception {
        Magazine<String> magazine = buildUnshardedMagazine(
                "CONCURRENT_FIRE_MAGAZINE", "CONCURRENT_FIRE_DATA", "CONCURRENT_FIRE_META");
        int records = 20;
        for (int i = 0; i < records; i++) {
            Assert.assertTrue(magazine.load("DATA_" + i));
        }

        ExecutorService executor = Executors.newFixedThreadPool(8);
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

            Assert.assertEquals(records, firedData.size());
            Assert.assertEquals(records, firedPointers.size());
            MetaData metaData = collectMetaData(magazine.getMetaData());
            Assert.assertEquals(records, metaData.getFirePointer());
            Assert.assertEquals(records, metaData.getFireCounter());
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
            Assert.assertTrue(magazine.load("DATA_" + i));
        }
        for (int pointer = 1; pointer <= 10; pointer++) {
            Assert.assertTrue(aerospikeClient.delete(
                    aerospikeClient.getWritePolicyDefault(),
                    new Key("NAMESPACE", "FARM_ID_MANY_HOLES_DATA",
                            "MAGAZINE_WITH_MANY_POINTER_HOLES_" + pointer)));
        }

        Assert.assertEquals("DATA_10", magazine.fire().getData());
    }

    @Test
    public void interruptedFireRetryPreservesInterruptStatus() throws ExecutionException, RetryException {
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("INTERRUPTED_FIRE_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();

        Assert.assertTrue(magazine.load("DATA"));
        deleteOnlyLoadedRecord(magazine);
        Thread.currentThread().interrupt();
        try {
            assertMagazineError(ErrorCode.RETRIES_EXHAUSTED, magazine::fire);
            Assert.assertTrue(Thread.currentThread().isInterrupted());
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

        Assert.assertNull(storage.getLockManager());
        Assert.assertNull(storage.getLockLevel());
        Assert.assertTrue(magazine.load("DATA"));
        Assert.assertTrue(magazine.load("DATA"));
        Assert.assertEquals(2, collectMetaData(magazine.getMetaData()).getLoadCounter());
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
        Set<MagazineData<String>> magazineDataSet = magazine.peek(
                Map.of(
                        Integer.parseInt(shardId.substring(shardId.indexOf(Constants.KEY_DELIMITER) + 1)),
                        Set.of(randomPointerInShard),
                        32,
                        Set.of(10L, 20L)
                )
        );
        Assert.assertEquals(1, magazineDataSet.size());
        MagazineData<String> magazineData = magazineDataSet.iterator().next();
        Assert.assertNotNull(magazineData.getData());
        Assert.assertEquals("MAGAZINE_ID1", magazineData.getMagazineIdentifier());
        Assert.assertEquals(randomPointerInShard, magazineData.getFirePointer());
        Assert.assertEquals(Integer.valueOf(
                Integer.parseInt(shardId.substring(shardId.indexOf(Constants.KEY_DELIMITER) + 1))),
                magazineData.getShard());
    }

    @Test
    public void longMagazineTest() {
        Magazine<Long> magazine = magazineManager.getMagazine("MAGAZINE_ID2");

        MetaData metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(0, metaData.getFireCounter());
        Assert.assertEquals(0, metaData.getLoadCounter());
        Assert.assertEquals(0, metaData.getFirePointer());
        Assert.assertEquals(0, metaData.getLoadPointer());

        boolean success = magazine.load(12L);
        Assert.assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(0, metaData.getFireCounter());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(0, metaData.getFirePointer());
        Assert.assertEquals(1, metaData.getLoadPointer());

        MagazineData<Long> data = magazine.fire();
        Assert.assertEquals(12L, data.getData()
                .longValue());

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(1, metaData.getFireCounter());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(1, metaData.getFirePointer());
        Assert.assertEquals(1, metaData.getLoadPointer());

        success = magazine.reload(12L);
        Assert.assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(2, metaData.getLoadPointer());
    }

    @Test
    public void integerMagazineTest() {
        Magazine<Integer> magazine = magazineManager.getMagazine("MAGAZINE_ID3");

        MetaData metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(0, metaData.getFireCounter());
        Assert.assertEquals(0, metaData.getLoadCounter());
        Assert.assertEquals(0, metaData.getFirePointer());
        Assert.assertEquals(0, metaData.getLoadPointer());

        boolean success = magazine.load(12);
        Assert.assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(0, metaData.getFireCounter());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(0, metaData.getFirePointer());
        Assert.assertEquals(1, metaData.getLoadPointer());

        success = magazine.reload(12);
        Assert.assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(2, metaData.getLoadPointer());
    }

    @Test
    public void exceptionsTest() {

        try {
            Magazine<Integer> magazine = magazineManager.getMagazine("MAGAZINE_ID1");
            magazine.load(12);
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.DATA_TYPE_MISMATCH, e.getErrorCode());
        }

        try {
            magazineManager.getMagazine("MAGAZINE1234");
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.MAGAZINE_NOT_FOUND, e.getErrorCode());
        }
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
        Assert.assertTrue(numberMagazine.load(1));
    }

    @Test
    public void notImplementedGlobalScopeTest() throws ExecutionException, RetryException {
        try {
            Magazine.<Long>builder()
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
                    .build();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.NOT_IMPLEMENTED, e.getErrorCode());
        }
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
        Assert.assertTrue(aerospikeClient.delete(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_DATA_SET", key)));
    }

    private void assertMagazineError(final ErrorCode errorCode, final org.junit.function.ThrowingRunnable action) {
        MagazineException exception = Assert.assertThrows(MagazineException.class, action);
        Assert.assertEquals(errorCode, exception.getErrorCode());
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
