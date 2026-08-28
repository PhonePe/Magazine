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
import com.aerospike.client.Host;
import com.aerospike.client.Key;
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

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.junit.*;
import org.testcontainers.containers.GenericContainer;

/**
 * @author shantanu.tiwari
 */
@SuppressWarnings("unchecked")
public class MagazineTest {

    private final Random random = new SecureRandom();
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
        Assert.assertFalse(magazineDataSet.isEmpty());
        Assert.assertNotNull(magazineDataSet.stream()
                .findAny()
                .get()
                .getData());
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
    public void configurationValidationTest() throws ExecutionException, RetryException {
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
