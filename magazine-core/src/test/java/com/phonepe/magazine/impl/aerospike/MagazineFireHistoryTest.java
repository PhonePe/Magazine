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
import com.aerospike.client.Record;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.FireCheckpoint;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Delivery-time checkpoints and {@link Magazine#firePointerBefore(Instant)}.
 * <p>
 * The property under test throughout is one-directional: a reported pointer must be one the shard
 * had certainly reached by the reported time. Under-reporting is fine and expected; over-reporting
 * would let a caller take a record away from a consumer that is still working on it.
 * <p>
 * Windows are one second wide here so rollover is observable without the tests taking minutes.
 */
class MagazineFireHistoryTest extends AerospikeMagazineTestBase {

    private static final String NAMESPACE = "NAMESPACE";
    private static final String META_SET = "FARM_ID_META_SET";
    private static final int WINDOW_SECONDS = 1;

    @Test
    @DisplayName("fire history is off unless asked for, and asking it anything then fails loudly")
    void disabledByDefault() {
        final Magazine<String> magazine = magazine("HIST_DISABLED", "HIST_DISABLED_SET", false, 4, 8);
        magazine.load("one");
        magazine.fire();

        assertMagazineError(ErrorCode.NOT_ENABLED, () -> magazine.firePointerBefore(Instant.now()));
        assertNull(history(magazine, 0), "no checkpoint bin should exist when the feature is off");
    }

    @Test
    @DisplayName("one checkpoint per shard per window, however many times it is asked for")
    void recordsOneCheckpointPerWindow() {
        final int shards = 4;
        // A wide window so the whole test runs inside one, making "same window" unambiguous.
        final Magazine<String> magazine = magazine("HIST_ONE", "HIST_ONE_SET", true, shards, 8, 60);
        loadAndFire(magazine, 3);

        // Shards only get a metadata record once something is loaded to them, and shard choice is
        // random, so the expectation is one checkpoint per shard that exists - not per configured shard.
        final int withRecords = shardsWithMetadata(magazine, shards);
        assertTrue(withRecords > 0, "at least one shard should have been written to");

        magazine.firePointerBefore(Instant.now());
        assertEquals(withRecords, totalCheckpoints(magazine, shards),
                "one window should record exactly one checkpoint on each shard that exists");

        magazine.firePointerBefore(Instant.now());
        magazine.firePointerBefore(Instant.now());
        assertEquals(withRecords, totalCheckpoints(magazine, shards),
                "further calls inside the same window must not add entries");
    }

    @Test
    @DisplayName("a new window adds a checkpoint without disturbing the old one")
    void newWindowAddsACheckpoint() throws Exception {
        final Magazine<String> magazine = magazine("HIST_ROLL", "HIST_ROLL_SET", true, 1, 8);
        loadAndFire(magazine, 2);
        magazine.firePointerBefore(Instant.now());
        final Map<Long, Long> first = new TreeMap<>(history(magazine, 0));

        Thread.sleep(1_100);
        loadAndFire(magazine, 2);
        magazine.firePointerBefore(Instant.now());

        // Counted as "more than before" rather than "exactly two": the active-shard refresh records
        // on its own schedule, so the number of windows a test crosses is not something it controls.
        final Map<Long, Long> second = new TreeMap<>(history(magazine, 0));
        assertTrue(second.size() > first.size(), "a later window should have been appended");
        first.forEach((window, pointer) -> assertEquals(pointer, second.get(window),
                "an existing checkpoint must never be rewritten"));
    }

    @Test
    @DisplayName("history is capped by count, so it cannot grow without bound")
    void historyIsBoundedByCount() throws Exception {
        final int cap = 3;
        final Magazine<String> magazine = magazine("HIST_CAP", "HIST_CAP_SET", true, 1, cap);
        for (int round = 0; round < cap + 3; round++) {
            loadAndFire(magazine, 1);
            magazine.firePointerBefore(Instant.now());
            Thread.sleep(1_100);
        }
        assertEquals(cap, history(magazine, 0).size(),
                "the newest entries are kept and everything past the cap is dropped");
    }

    @Test
    @DisplayName("a recorded pointer never runs ahead of the shard's actual fire pointer")
    void checkpointsNeverOverReport() {
        final Magazine<String> magazine = magazine("HIST_SAFE", "HIST_SAFE_SET", true, 1, 8);
        loadAndFire(magazine, 5);
        magazine.firePointerBefore(Instant.now());

        final Record record = record(magazine, 0);
        final long actual = record.getLong(AerospikeConstants.FIRE_POINTER);
        history(magazine, 0).values().forEach(recorded -> assertTrue(recorded <= actual,
                "checkpoint " + recorded + " must not exceed the fire pointer " + actual));
    }

    @Test
    @DisplayName("the answer is the newest checkpoint at or before the instant asked about")
    void returnsNewestCheckpointNotAfterTheInstant() throws Exception {
        final Magazine<String> magazine = magazine("HIST_PICK", "HIST_PICK_SET", true, 1, 8);
        loadAndFire(magazine, 2);
        magazine.firePointerBefore(Instant.now());
        final Instant afterFirstBurst = Instant.now();
        final long pointerAfterFirstBurst = record(magazine, 0).getLong(AerospikeConstants.FIRE_POINTER);

        Thread.sleep(1_100);
        loadAndFire(magazine, 4);
        magazine.firePointerBefore(Instant.now());

        final FireCheckpoint checkpoint = magazine.firePointerBefore(afterFirstBurst).get("SHARD_0");
        assertNotNull(checkpoint, "the first burst's window is old enough to answer with");
        assertFalse(checkpoint.recordedAt().isAfter(afterFirstBurst),
                "the answer must come from a window that had already closed by then");
        assertTrue(checkpoint.firePointer() <= pointerAfterFirstBurst,
                "the second burst's pointer must not be used to answer an earlier question");
    }

    @Test
    @DisplayName("a young magazine reports nothing rather than failing")
    void youngHistoryReturnsAbsent() {
        final Magazine<String> magazine = magazine("HIST_YOUNG", "HIST_YOUNG_SET", true, 1, 8);
        loadAndFire(magazine, 1);

        // Nothing has been recorded this far back, and the map is nowhere near capacity, so the
        // magazine simply has not lived long enough. That is normal and must not be an error.
        assertTrue(magazine.firePointerBefore(Instant.now().minus(Duration.ofHours(1))).isEmpty());
    }

    @Test
    @DisplayName("a full history that cannot reach back far enough is a configuration error, not an empty answer")
    void evictedHistoryFailsLoudly() throws Exception {
        final int cap = 2;
        final Magazine<String> magazine = magazine("HIST_EVICTED", "HIST_EVICTED_SET", true, 1, cap);
        final Instant longAgo = Instant.now().minus(Duration.ofHours(1));
        for (int round = 0; round < cap + 1; round++) {
            loadAndFire(magazine, 1);
            magazine.firePointerBefore(Instant.now());
            Thread.sleep(1_100);
        }

        // At capacity with every retained entry newer than the question: real history was
        // discarded. Returning empty would look identical to "nothing to do" and would silently
        // stop a caller from ever acting. It is the instant that is out of range, not the config -
        // the same magazine answers a recent instant perfectly well.
        assertMagazineError(ErrorCode.INVALID_REQUEST, () -> magazine.firePointerBefore(longAgo));
    }

    @Test
    @DisplayName("reading advances history, so a burst that is never fired from again stays reachable")
    void readingRecordsACheckpoint() throws Exception {
        final Magazine<String> magazine = magazine("HIST_BURST", "HIST_BURST_SET", true, 1, 8);
        loadAndFire(magazine, 3);
        magazine.firePointerBefore(Instant.now());
        final long newestBefore = newestWindowKey(magazine);

        Thread.sleep(1_100);
        // No load, no fire: the magazine is idle. Reading alone must still move history forward,
        // otherwise a magazine that took one burst and was then abandoned would hold a single
        // checkpoint predating the burst and its orphans would never become visible.
        magazine.firePointerBefore(Instant.now());

        // The property, not a count. The active-shard refresh records on its own schedule, so how
        // many windows exist by this point is not something the test controls - under load the
        // preceding loadAndFire can contribute one of its own. What the read must guarantee is that
        // a window strictly newer than anything present beforehand now exists.
        assertTrue(newestWindowKey(magazine) < newestBefore,
                "the read should have recorded a newer window by itself; keys are negated window "
                        + "timestamps, so newest sorts lowest");
    }

    @Test
    @DisplayName("consuming records checkpoints on its own, without anyone reading them")
    void firingRecordsCheckpoints() {
        final Magazine<String> magazine = magazine("HIST_FIRE", "HIST_FIRE_SET", true, 1, 8);
        loadAndFire(magazine, 4);

        // The active-shard cache refreshes asynchronously, so this arrives shortly after the fire
        // rather than during it.
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    loadAndFire(magazine, 1);
                    assertFalse(history(magazine, 0).isEmpty(),
                            "firing alone should eventually record a checkpoint");
                });
    }

    @Test
    @DisplayName("a shard that only appears mid-window still gets that window's checkpoint")
    void lateShardIsCheckpointedInTheSameWindow() {
        final int shards = 8;
        // One window wide enough to hold the whole test, so nothing here can roll into the next.
        final Magazine<String> magazine = magazine("HIST_LATE", "HIST_LATE_SET", true, shards, 8, 60);
        loadAndFire(magazine, 1);
        magazine.firePointerBefore(Instant.now());
        final int firstPass = shardsWithMetadata(magazine, shards);
        assertEquals(firstPass, totalCheckpoints(magazine, shards));

        // Shard choice is random, so keep loading until a shard that did not exist during the first
        // pass does now.
        for (int attempt = 0; attempt < 50 && shardsWithMetadata(magazine, shards) == firstPass; attempt++) {
            loadAndFire(magazine, 1);
        }
        final int secondPass = shardsWithMetadata(magazine, shards);
        assertTrue(secondPass > firstPass, "expected a shard to appear after the first pass");

        magazine.firePointerBefore(Instant.now());
        assertEquals(secondPass, totalCheckpoints(magazine, shards),
                "a shard that appeared after the window was first written must still be recorded; "
                        + "claiming the window per magazine would leave it blank until the next one");
    }

    @Test
    @DisplayName("the whole retained history reads back newest first, and reading it records nothing")
    void fireHistoryReturnsEverythingRetainedWithoutRecording() throws Exception {
        final Magazine<String> magazine = magazine("HIST_ALL", "HIST_ALL_SET", true, 1, 8);
        for (int round = 0; round < 3; round++) {
            loadAndFire(magazine, 1);
            magazine.firePointerBefore(Instant.now());
            Thread.sleep(1_100);
        }

        final List<FireCheckpoint> checkpoints = magazine.fireHistory().get("SHARD_0");
        assertNotNull(checkpoints);
        assertEquals(history(magazine, 0).size(), checkpoints.size(), "every retained entry is returned");
        for (int i = 1; i < checkpoints.size(); i++) {
            assertTrue(checkpoints.get(i - 1).recordedAt().isAfter(checkpoints.get(i).recordedAt()),
                    "newest first");
            assertTrue(checkpoints.get(i - 1).firePointer() >= checkpoints.get(i).firePointer(),
                    "pointers only ever move forward");
        }

        // A console browsing history must not write to the magazine it is inspecting.
        final int before = history(magazine, 0).size();
        Thread.sleep(1_100);
        magazine.fireHistory();
        assertEquals(before, history(magazine, 0).size(), "reading history must not record a window");
    }

    @Test
    @DisplayName("reading the whole history is refused when the feature is off")
    void fireHistoryIsRefusedWhenDisabled() {
        final Magazine<String> magazine = magazine("HIST_ALL_OFF", "HIST_ALL_OFF_SET", false, 1, 8);
        magazine.load("one");
        assertMagazineError(ErrorCode.NOT_ENABLED, magazine::fireHistory);
    }

    @Test
    @DisplayName("the checkpoint map stays off the hot metadata read path")
    void checkpointsAreNotProjectedOntoMetadataReads() {
        assertFalse(Arrays.asList(AerospikeConstants.getMetadataBins()).contains(AerospikeConstants.FIRE_HISTORY),
                "metadata batch reads fan out per shard on every refresh; carrying the checkpoint "
                        + "map there would put it on the hottest read in the system");
    }

    @Test
    @DisplayName("history sizing is validated at construction")
    void configurationIsValidated() {
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION,
                () -> storageWithHistory("BAD_WINDOW_SET", true, 0, 8));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION,
                () -> storageWithHistory("BAD_ENTRIES_LOW_SET", true, 1, 1));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION,
                () -> storageWithHistory("BAD_ENTRIES_HIGH_SET", true, 1,
                        AerospikeConstants.MAX_FIRE_HISTORY_ENTRIES + 1));
    }

    // --- helpers ---------------------------------------------------------------------------

    private Magazine<String> magazine(final String identifier, final String setName,
                                      final boolean enabled, final int shards, final int entries) {
        return magazine(identifier, setName, enabled, shards, entries, WINDOW_SECONDS);
    }

    private Magazine<String> magazine(final String identifier, final String setName,
                                      final boolean enabled, final int shards, final int entries,
                                      final int windowSeconds) {
        return Magazine.<String>builder()
                .magazineIdentifier(identifier)
                .baseMagazineStorage(storageWithHistory(setName, enabled, windowSeconds, entries, shards))
                .build();
    }

    private AerospikeStorage<String> storageWithHistory(final String setName, final boolean enabled,
                                                        final int windowSeconds, final int entries) {
        return storageWithHistory(setName, enabled, windowSeconds, entries, 1);
    }

    private AerospikeStorage<String> storageWithHistory(final String setName, final boolean enabled,
                                                        final int windowSeconds, final int entries,
                                                        final int shards) {
        return buildStorage(AerospikeStorageConfig.builder()
                        .namespace(NAMESPACE)
                        .dataSetName(setName)
                        .metaSetName("META_SET")
                        .recordTtl(30 * 24 * 60 * 60)
                        .metaDataTtl(2 * 30 * 24 * 60 * 60)
                        .shards(shards)
                        // Refresh often, so the fire path exercises the recorder within a test's life.
                        .activeShardRefreshSeconds(1)
                        .fireHistoryEnabled(enabled)
                        .fireHistoryWindowSeconds(windowSeconds)
                        .fireHistoryEntries(entries)
                        .build(),
                String.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient);
    }

    private void loadAndFire(final Magazine<String> magazine, final int count) {
        for (int i = 0; i < count; i++) {
            magazine.load("message-" + random.nextLong());
        }
        for (int i = 0; i < count; i++) {
            fireWhenVisible(magazine);
        }
    }

    /**
     * Draining a shard suppresses it from the active-shard cache, and the cache only reloads
     * asynchronously on access. A load immediately after a drain is therefore not yet visible to
     * fire(), which is expected behaviour rather than something these tests should assert on.
     */
    private void fireWhenVisible(final Magazine<String> magazine) {
        Awaitility.await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    try {
                        return magazine.fire() != null;
                    } catch (MagazineException e) {
                        if (e.getErrorCode() == ErrorCode.NOTHING_TO_FIRE) {
                            return false;
                        }
                        throw e;
                    }
                });
    }

    private Record record(final Magazine<String> magazine, final int shard) {
        final String name = magazine.getShards() <= 1
                ? magazine.getMagazineIdentifier() + "_" + AerospikeConstants.METADATA
                : magazine.getMagazineIdentifier() + "_SHARD_" + shard + "_" + AerospikeConstants.METADATA;
        return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(NAMESPACE, META_SET, name));
    }

    @SuppressWarnings("unchecked")
    private Map<Long, Long> history(final Magazine<String> magazine, final int shard) {
        final Record record = record(magazine, shard);
        if (record == null) {
            return null;
        }
        final Object raw = record.getValue(AerospikeConstants.FIRE_HISTORY);
        return raw instanceof Map ? (Map<Long, Long>) raw : null;
    }

    private int shardsWithMetadata(final Magazine<String> magazine, final int shards) {
        int found = 0;
        for (int shard = 0; shard < shards; shard++) {
            if (record(magazine, shard) != null) {
                found++;
            }
        }
        return found;
    }

    /**
     * Keys are negated window timestamps on a KEY_ORDERED map, so the newest window is the lowest
     * key. Returns 0 when nothing has been recorded, which no real window can equal.
     */
    private long newestWindowKey(final Magazine<String> magazine) {
        final Map<Long, Long> history = history(magazine, 0);
        assertNotNull(history, "expected a checkpoint map on the pointer record");
        return history.keySet().stream().mapToLong(Long::longValue).min().orElse(0L);
    }

    private int totalCheckpoints(final Magazine<String> magazine, final int shards) {        int total = 0;
        for (int shard = 0; shard < shards; shard++) {
            final Map<Long, Long> history = history(magazine, shard);
            total += history == null ? 0 : history.size();
        }
        return total;
    }
}
