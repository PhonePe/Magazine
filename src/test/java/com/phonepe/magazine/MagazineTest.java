package com.phonepe.magazine;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.github.rholder.retry.RetryException;
import com.google.common.collect.ImmutableList;
import com.phonepe.magazine.common.Constants;
import com.phonepe.magazine.common.MagazineData;
import com.phonepe.magazine.common.MetaData;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.impl.aerospike.AerospikeStorage;
import com.phonepe.magazine.impl.aerospike.AerospikeStorageConfig;
import com.phonepe.magazine.scope.MagazineScope;
import com.phonepe.magazine.util.MockAerospikeClient;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * @author shantanu.tiwari
 */
@SuppressWarnings("unchecked")
public class MagazineTest {

    private final Random random = new SecureRandom();
    private MagazineManager magazineManager;
    private MockAerospikeClient aerospikeClient = new MockAerospikeClient();

    @Before
    public void setup() throws Exception {
        magazineManager = new MagazineManager("CLIENT_ID");
        magazineManager.refresh(ImmutableList.of(Magazine.<String>builder()
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
                        .build()));
    }

    @Test
    public void stringMagazineTest() {
        Magazine<String> magazine = magazineManager.getMagazine("MAGAZINE_ID1");
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

        MagazineData<Integer> data = magazine.fire();
        Assert.assertEquals(12, data.getData()
                .intValue());

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(1, metaData.getFireCounter());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(1, metaData.getFirePointer());
        Assert.assertEquals(1, metaData.getLoadPointer());

        success = magazine.reload(12);
        Assert.assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(2, metaData.getLoadPointer());
    }

    @Test
    public void exceptionsTest() throws Exception {

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
                            .clientId("CLIENT_ID")
                            .scope(MagazineScope.LOCAL)
                            .build())
                    .build();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.NOT_IMPLEMENTED, e.getErrorCode());
        }
    }

    @Test
    public void extendedExceptionsTest() throws Exception {
        MockAerospikeClient aerospikeClientSpyed = Mockito.spy(aerospikeClient);
        doReturn(false).when(aerospikeClientSpyed)
                .delete(any(), any());

        final Magazine<Long> magazine = Magazine.<Long>builder()
                .magazineIdentifier("MAGAZINE_ID")
                .baseMagazineStorage(AerospikeStorage.<Long>builder()
                        .clazz(Long.class)
                        .storageConfig(AerospikeStorageConfig.builder()
                                .dataSetName("DATA_SET")
                                .metaSetName("META_SET")
                                .namespace("NAMESPACE")
                                .shards(16)
                                .build())
                        .aerospikeClient(aerospikeClientSpyed)
                        .enableDeDupe(true)
                        .clientId("CLIENT_ID")
                        .scope(MagazineScope.LOCAL)
                        .build())
                .build();
        try {
            magazine.load(1L);
            magazine.load(1L);
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.ACTION_DENIED_PARALLEL_ATTEMPT, e.getErrorCode());
        }

        try {
            magazine.load(2L);
            magazine.reload(1L);
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.ACTION_DENIED_PARALLEL_ATTEMPT, e.getErrorCode());
        }

        doThrow(AerospikeException.class).when(aerospikeClientSpyed)
                .get(any(), ArgumentMatchers.<Key[]>any());
        try {
            magazine.getMetaData();
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.RETRIES_EXHAUSTED, e.getErrorCode());
        }
        try {
            magazine.fire();
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.CONNECTION_ERROR, e.getErrorCode());
        }

        doThrow(RuntimeException.class).when(aerospikeClientSpyed)
                .get(any(), ArgumentMatchers.<Key[]>any());
        try {
            magazine.getMetaData();
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.CONNECTION_ERROR, e.getErrorCode());
        }
        try {
            magazine.fire();
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.CONNECTION_ERROR, e.getErrorCode());
        }
        try {
            magazine.peek(anyMap());
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.CONNECTION_ERROR, e.getErrorCode());
        }
    }

    private <T> BaseMagazineStorage<T> buildMagazineStorage(Class<T> clazz) {
        return AerospikeStorage.<T>builder()
                .clazz(clazz)
                .storageConfig(AerospikeStorageConfig.builder()
                        .dataSetName("DATA_SET")
                        .metaSetName("META_SET")
                        .namespace("NAMESPACE")
                        .shards(16)
                        .build())
                .aerospikeClient(aerospikeClient)
                .enableDeDupe(true)
                .clientId("CLIENT_ID")
                .scope(MagazineScope.LOCAL)
                .build();
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
