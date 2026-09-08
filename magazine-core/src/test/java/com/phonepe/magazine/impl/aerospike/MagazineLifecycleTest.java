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

import com.aerospike.client.Key;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.MagazineManager;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end load / fire / delete / peek over the payload types the storage supports.
 */
@SuppressWarnings("unchecked")
class MagazineLifecycleTest extends AerospikeMagazineTestBase {

    private MagazineManager magazineManager;

    @BeforeEach
    void setup() {
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
    void stringMagazineTest() {
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
    void magazineCannotDeleteAnotherMagazinesData() {
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
    void magazinePeekTest() {
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
    void peekRejectsShardOutsideMagazineRange() {
        Magazine<String> magazine = magazineManager.getMagazine("MAGAZINE_ID1");
        magazine.load("DATA1");

        // 32 is beyond this magazine's 16 shards. Previously this silently produced a key that
        // could never match; it is a caller error and must surface as one.
        assertMagazineError(ErrorCode.INVALID_SHARDS,
                () -> magazine.peek(Map.of(32, Set.of(10L, 20L))));
    }

    @Test
    void longMagazineTest() {
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
    void integerMagazineTest() {
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
    void exceptionsTest() {
        // getMagazine() is resolved outside the lambda: MAGAZINE_ID1 exists, so it cannot throw
        // here, but a lambda that lexically contains two calls that can each raise a
        // MagazineException would leave it ambiguous which one this test is pinning - load(12) is.
        Magazine<Integer> magazine = magazineManager.getMagazine("MAGAZINE_ID1");
        MagazineException typeMismatch = assertThrows(MagazineException.class, () -> magazine.load(12));
        assertEquals(ErrorCode.DATA_TYPE_MISMATCH, typeMismatch.getErrorCode());

        MagazineException notFound = assertThrows(MagazineException.class,
                () -> magazineManager.getMagazine("MAGAZINE1234"));
        assertEquals(ErrorCode.MAGAZINE_NOT_FOUND, notFound.getErrorCode());
    }

    @Test
    void payloadTypeValidationTest() {
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
}
