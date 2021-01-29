package com.phonepe.growth.magazine;

import com.google.common.collect.ImmutableList;
import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import com.phonepe.growth.magazine.impl.aerospike.AerospikeStorage;
import com.phonepe.growth.magazine.impl.aerospike.AerospikeStorageConfig;
import com.phonepe.growth.magazine.util.MockAerospikeClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

/**
 * @author shantanu.tiwari
 */
@SuppressWarnings("unchecked")
public class MagazineTest {
    private MagazineManager magazineManager;
    private MockAerospikeClient aerospikeClient = new MockAerospikeClient();

    @Before
    public void setup() throws Exception {
        magazineManager = new MagazineManager("CLIENT_ID");
        magazineManager.refresh(ImmutableList.of(
                Magazine.builder()
                        .magazineIdentifier("MAGAZINE_ID1")
                        .clientId("CLIENT_ID")
                        .baseMagazineStorage(buildMagazineStorage(String.class))
                        .build(),
                Magazine.builder()
                        .magazineIdentifier("MAGAZINE_ID2")
                        .clientId("CLIENT_ID")
                        .baseMagazineStorage(buildMagazineStorage(Long.class))
                        .build(),
                Magazine.builder()
                        .magazineIdentifier("MAGAZINE_ID3")
                        .clientId("CLIENT_ID")
                        .baseMagazineStorage(buildMagazineStorage(Integer.class))
                        .build(),
                Magazine.builder()
                        .magazineIdentifier("MAGAZINE_ID4")
                        .clientId("CLIENT_ID")
                        .baseMagazineStorage(buildMagazineStorage(String.class))
                        .build()
                )
        );
    }

    @Test
    public void stringMagazineTest() {
        Magazine magazine = magazineManager.getMagazine("MAGAZINE_ID1");
        Magazine magazine2 = magazineManager.getMagazine("MAGAZINE_ID4");

        MetaData metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(0, metaData.getFireCounter());
        Assert.assertEquals(0, metaData.getLoadCounter());
        Assert.assertEquals(0, metaData.getFirePointer());
        Assert.assertEquals(0, metaData.getLoadPointer());

        boolean success = magazine.load("DATA1");
        Assert.assertTrue(success);

        success = magazine2.load("DATA1");
        Assert.assertTrue(success);

        metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(0, metaData.getFireCounter());
        Assert.assertEquals(1, metaData.getLoadCounter());
        Assert.assertEquals(0, metaData.getFirePointer());
        Assert.assertEquals(1, metaData.getLoadPointer());

        Optional<String> data = magazine.fire();
        Assert.assertTrue(data.isPresent());
        Assert.assertEquals("DATA1", data.get());

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
    public void longMagazineTest() {
        Magazine magazine = magazineManager.getMagazine("MAGAZINE_ID2");

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

        Optional<Long> data = magazine.fire();
        Assert.assertTrue(data.isPresent());
        Assert.assertEquals(12L, data.get().longValue());

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
        Magazine magazine = magazineManager.getMagazine("MAGAZINE_ID3");

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

        Optional<Integer> data = magazine.fire();
        Assert.assertTrue(data.isPresent());
        Assert.assertEquals(12, data.get().intValue());

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
            Magazine magazine = magazineManager.getMagazine("MAGAZINE_ID1");
            magazine.load(12);
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.DATA_TYPE_MISMATCH, e.getErrorCode());
        }

        try {
            MagazineManager newMagazineManager = new MagazineManager("CLIENT_ID2");
            newMagazineManager.refresh(ImmutableList.of(
                    Magazine.builder()
                            .magazineIdentifier("MAGAZINE_ID1")
                            .clientId("CLIENT_ID")
                            .baseMagazineStorage(buildMagazineStorage(Map.class))
                            .build())
            );
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.UNSUPPORTED_CLASS_FOR_DEDUPE, e.getErrorCode());
        }

        try {
            magazineManager.getMagazine("MAGAZINE1234");
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.MAGAZINE_NOT_FOUND, e.getErrorCode());
        }
    }

    private BaseMagazineStorage buildMagazineStorage(Class clazz) {
        return AerospikeStorage.<String>builder()
                .clazz(clazz)
                .storageConfig(AerospikeStorageConfig.builder()
                        .dataSetName("DATA_SET")
                        .metaSetName("META_SET")
                        .namespace("NAMESPACE")
                        .shards(16)
                        .build())
                .aerospikeClient(aerospikeClient)
                .enableDeDupe(true)
                .build();
    }

    public MetaData collectMetaData(Map<String, MetaData> metaDataMap) {
        return MetaData.builder()
                .loadPointer(metaDataMap.values().stream().mapToLong(MetaData::getLoadPointer).sum())
                .loadCounter(metaDataMap.values().stream().mapToLong(MetaData::getLoadCounter).sum())
                .firePointer(metaDataMap.values().stream().mapToLong(MetaData::getFirePointer).sum())
                .fireCounter(metaDataMap.values().stream().mapToLong(MetaData::getFireCounter).sum())
                .build();
    }

}
