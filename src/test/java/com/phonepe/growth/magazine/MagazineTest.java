package com.phonepe.growth.magazine;

import com.phonepe.growth.dlm.DistributedLockManager;
import com.phonepe.growth.dlm.core.LockMode;
import com.phonepe.growth.dlm.impl.aerospike.AerospikeLock;
import com.phonepe.growth.dlm.impl.aerospike.AerospikeStore;
import com.phonepe.growth.magazine.commands.LockCommands;
import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import com.phonepe.growth.magazine.impl.aerospike.AerospikeStorage;
import com.phonepe.growth.magazine.util.MockAerospikeClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
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
        magazineManager.refresh(Collections.singletonList(Magazine.builder()
                .magazineIdentifier("MAGAZINE_ID")
                .clientId("CLIENT_ID")
                .baseMagazineStorage(buildMagazineStorage())
                .build()));
    }

    @Test
    public void magazineTest() {
        Magazine magazine = magazineManager.getMagazine("MAGAZINE_ID");

        MetaData metaData = collectMetaData(magazine.getMetaData());
        Assert.assertEquals(0, metaData.getFireCounter());
        Assert.assertEquals(0, metaData.getLoadCounter());
        Assert.assertEquals(0, metaData.getFirePointer());
        Assert.assertEquals(0, metaData.getLoadPointer());

        boolean success = magazine.load("DATA1");
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
    public void exceptionsTest() {
        try {
            LockCommands lockCommands = new LockCommands(new DistributedLockManager("CLIENT_ID", AerospikeLock.builder()
                    .mode(LockMode.EXCLUSIVE)
                    .store(AerospikeStore.builder()
                            .aerospikeClient(aerospikeClient)
                            .namespace("NAMESPACE")
                            .setname("distributed_lock")
                            .build())
                    .build()));

            lockCommands.acquireLock("LOCK_ID");
            lockCommands.acquireLock("LOCK_ID");
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.ACTION_DENIED_PARALLEL_ATTEMPT, e.getErrorCode());
        }

        try {
            magazineManager.getMagazine("MAGAZINE_ID2");
            Assert.fail();
        } catch (MagazineException e) {
            Assert.assertEquals(ErrorCode.MAGAZINE_NOT_FOUND, e.getErrorCode());
        }
    }

    private BaseMagazineStorage buildMagazineStorage() {
        return AerospikeStorage.<String>builder()
                .klass(String.class)
                .dataSetName("DATA_SET")
                .metaSetName("META_SET")
                .aerospikeClient(aerospikeClient)
                .namespace("NAMESPACE")
                .recordTtl(10000)
                .enableDeDupe(true)
                .shards(16)
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
