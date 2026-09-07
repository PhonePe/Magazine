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
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.exception.ErrorCode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * fire() semantics: hole skipping, the exhausted-versus-drained distinction, and that concurrent
 * consumers each claim a record exactly once.
 */
class MagazineFireTest extends AerospikeMagazineTestBase {

    @Test
    public void emptyMagazineReturnsNothingToFire() {
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("EMPTY_CACHE_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();

        assertMagazineError(ErrorCode.NOTHING_TO_FIRE, magazine::fire);
    }

    @Test
    public void missingRecordReturnsNothingToFireAfterShardIsExhausted() {
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("MISSING_RECORD_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, false))
                .build();

        assertTrue(magazine.load("DATA"));
        deleteOnlyLoadedRecord(magazine);
        assertMagazineError(ErrorCode.NOTHING_TO_FIRE, magazine::fire);
    }

    @Test
    public void fireSkipsMissingPointerWithinActiveShard() {
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
    public void fireSkipsMoreThanFiveMissingPointers() {
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
    public void fireGivesUpWithRetriesExhaustedWhenHoleBudgetIsSpent() {
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
    public void fireSurfacesMissingMetadataRatherThanHidingTheShard() {
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
    public void interruptedFireRetryPreservesInterruptStatus() {
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
}
