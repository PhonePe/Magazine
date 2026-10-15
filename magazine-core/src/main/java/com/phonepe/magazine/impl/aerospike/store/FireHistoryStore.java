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

package com.phonepe.magazine.impl.aerospike.store;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.phonepe.magazine.entity.FireCheckpoint;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineExceptions;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikePolicies;
import com.phonepe.magazine.impl.aerospike.common.AerospikeRetryer;
import com.phonepe.magazine.impl.aerospike.common.ErrorMessage;
import com.phonepe.magazine.metrics.MagazineMetrics;
import com.phonepe.magazine.metrics.StorageOperation;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Delivery-time checkpoints: per-shard map of {@code -label -> fire pointer} (newest first) on the
 * pointer record. Recorded from the active-shard refresh and from {@code firePointerBefore}; at most
 * one per shard per window, labelled with the window end (never earlier than the observation).
 * Skipped when the window is already recorded or the pointer is unchanged. One batch write per
 * magazine; refresh writes wait for a random per-process offset in the first half of the window.
 * Best-effort: failures never fail the triggering call.
 */
@Slf4j
public final class FireHistoryStore {

    /** A second writer for the same label is a server-side no-op. */
    private static final MapPolicy CHECKPOINT_MAP_POLICY = new MapPolicy(
            MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY | MapWriteFlags.NO_FAIL);

    private static final Operation READ_NEWEST_CHECKPOINT = MapOperation.getByIndex(
            AerospikeConstants.FIRE_HISTORY, 0, MapReturnType.KEY_VALUE);

    private final IAerospikeClient client;
    private final AerospikeRetryer retryer;
    private final MagazineMetrics metrics;
    private final Function<MagazineContext, List<Key>> pointerKeys;

    private final boolean enabled;
    private final long windowMillis;
    private final int entries;

    private final BatchPolicy batchPolicy;
    private final BatchWritePolicy writePolicy;

    /** Bounded by the number of configured magazines. */
    private final Map<MagazineContext, MagazineState> states = new ConcurrentHashMap<>();

    /**
     * @param windowMillis recording window width; the resolution of every answer.
     * @param entries      checkpoints retained per shard, evicted by count.
     */
    public record FireHistoryConfig(boolean enabled, long windowMillis, int entries) {
    }

    public FireHistoryStore(final IAerospikeClient client,
            final AerospikeRetryer retryer,
            final MagazineMetrics metrics,
            final Function<MagazineContext, List<Key>> pointerKeys,
            final int metaDataTtl,
            final FireHistoryConfig config) {
        this.client = client;
        this.retryer = retryer;
        this.metrics = metrics;
        this.pointerKeys = pointerKeys;
        this.enabled = config.enabled();
        this.windowMillis = config.windowMillis();
        this.entries = config.entries();

        this.batchPolicy = AerospikePolicies.batchPolicy(client);
        // UPDATE_ONLY: never create a pointer record for an uninitialised shard.
        this.writePolicy = AerospikePolicies.batchWritePolicy(client);
        this.writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        this.writePolicy.expiration = metaDataTtl;
    }

    /** Extra refresh-read operation: only the newest checkpoint, as {@link #recordOnRefresh} needs. */
    public Optional<Operation> refreshReadOperation() {
        return enabled ? Optional.of(READ_NEWEST_CHECKPOINT) : Optional.empty();
    }

    /** Records from the refresh read (must include the newest checkpoint), after this process's offset. */
    public void recordOnRefresh(final MagazineContext context, final Record[] pointerRecords) {
        if (!enabled) {
            return;
        }
        final long now = System.currentTimeMillis();
        final Window window = Window.containing(now, windowMillis);
        if (now >= window.start() + state(context).getJitterMillis()) {
            record(context, pointerRecords, window);
        }
    }

    /**
     * Lower bound of each shard's fire pointer at {@code instant}; may lag, never leads.
     *
     * @return shard id to checkpoint; absent when no checkpoint is old enough yet.
     * @throws com.phonepe.magazine.exception.MagazineException {@code NOT_ENABLED} if disabled,
     *         {@code INVALID_REQUEST} if retained history does not reach {@code instant}.
     */
    public Map<String, FireCheckpoint> firePointerBefore(final MagazineContext context, final Instant instant) {
        requireEnabled(context);
        final Record[] records = readHistory(context);

        // Record first (undelayed) so history still advances even if resolving throws.
        record(context, records, Window.containing(System.currentTimeMillis(), windowMillis));

        final Map<String, FireCheckpoint> answers = new HashMap<>(capacityFor(context.getShards()));
        for (int shard = 0; shard < context.getShards(); shard++) {
            final int current = shard;
            resolve(context, current, checkpoints(records[current]), instant)
                    .ifPresent(checkpoint -> answers.put(context.shardId(current), checkpoint.toEntity()));
        }
        return answers;
    }

    /** Every retained checkpoint per shard, newest first. Reading never records. */
    public Map<String, List<FireCheckpoint>> history(final MagazineContext context) {
        requireEnabled(context);
        final Record[] records = readHistory(context);
        final Map<String, List<FireCheckpoint>> history = new HashMap<>(capacityFor(context.getShards()));
        for (int shard = 0; shard < context.getShards(); shard++) {
            final List<FireCheckpoint> checkpoints = checkpoints(records[shard]).stream()
                    .map(Checkpoint::toEntity)
                    .toList();
            if (!checkpoints.isEmpty()) {
                history.put(context.shardId(shard), checkpoints);
            }
        }
        return history;
    }

    // --- recording ---------------------------------------------------------------------------

    private void record(final MagazineContext context, final Record[] pointerRecords, final Window window) {
        final MagazineState state = state(context);
        final List<Integer> shards = shardsNeedingCheckpoint(context, pointerRecords, window, state);
        if (!shards.isEmpty()) {
            write(context, pointerRecords, shards, window, state);
        }
    }

    /** Shards that need this window's checkpoint, each already claimed for this process. */
    private static List<Integer> shardsNeedingCheckpoint(final MagazineContext context,
            final Record[] pointerRecords,
            final Window window,
            final MagazineState state) {
        final List<Integer> shards = new ArrayList<>();
        for (int shard = 0; shard < context.getShards(); shard++) {
            final Record pointers = pointerRecords[shard];
            if (Objects.nonNull(pointers)
                    && needsCheckpoint(pointers, window)
                    && state.claim(shard, window)) {
                shards.add(shard);
            }
        }
        return shards;
    }

    private static boolean needsCheckpoint(final Record pointers, final Window window) {
        final Optional<Checkpoint> newest = checkpoints(pointers).stream().findFirst();
        if (newest.isEmpty()) {
            return true;
        }
        final boolean windowAlreadyRecorded = newest.get().label() >= window.label();
        // Fire pointer is monotonic: an unchanged value stays true, so skip idle shards.
        final boolean pointerUnchanged = newest.get().firePointer() == firePointer(pointers);
        return !windowAlreadyRecorded && !pointerUnchanged;
    }

    /** One batch call for every shard; a failed shard's claim is released so it is retried. */
    private void write(final MagazineContext context,
            final Record[] pointerRecords,
            final List<Integer> shards,
            final Window window,
            final MagazineState state) {
        final List<Key> keys = pointerKeys.apply(context);
        final List<BatchRecord> writes = new ArrayList<>(shards.size());
        for (int shard : shards) {
            writes.add(checkpointWrite(keys.get(shard), window.label(), firePointer(pointerRecords[shard])));
        }

        try {
            metrics.aerospikeCall(context.getMagazineIdentifier(), StorageOperation.WRITE_FIRE_HISTORY);
            client.operate(batchPolicy, writes);
        } catch (Exception e) {
            shards.forEach(shard -> state.release(shard, window));
            log.warn("Could not record fire history for magazine {} window {}.",
                    context.getMagazineIdentifier(), window.start(), e);
            return;
        }

        for (int i = 0; i < writes.size(); i++) {
            final int resultCode = writes.get(i).resultCode;
            if (resultCode == ResultCode.OK) {
                continue;
            }
            final int shard = shards.get(i);
            state.release(shard, window);
            // A missing record is expected: the shard has none yet, or it expired.
            if (resultCode != ResultCode.KEY_NOT_FOUND_ERROR) {
                log.warn("Could not record fire history for magazine {} shard {} window {}: {}.",
                        context.getMagazineIdentifier(), shard, window.start(),
                        ResultCode.getResultString(resultCode));
            }
        }
    }

    private BatchWrite checkpointWrite(final Key key, final long label, final long firePointer) {
        return new BatchWrite(writePolicy, key, new Operation[]{
                MapOperation.put(CHECKPOINT_MAP_POLICY, AerospikeConstants.FIRE_HISTORY,
                        Value.get(Checkpoint.encode(label)), Value.get(firePointer)),
                // Evict by count, not age, so a long idle period does not wipe usable history.
                MapOperation.removeByIndexRange(AerospikeConstants.FIRE_HISTORY, entries, MapReturnType.NONE)});
    }

    private MagazineState state(final MagazineContext context) {
        return states.computeIfAbsent(context, this::newState);
    }

    private MagazineState newState(final MagazineContext context) {
        // Random offset in the first half of the window, fixed per magazine per process.
        final long span = windowMillis / 2;
        final long jitter = span <= 0 ? 0L : ThreadLocalRandom.current().nextLong(span);
        return new MagazineState(context.getShards(), jitter);
    }

    // --- reading -----------------------------------------------------------------------------

    private Record[] readHistory(final MagazineContext context) {
        return retryer.call(() -> {
            metrics.aerospikeCall(context.getMagazineIdentifier(), StorageOperation.READ_FIRE_HISTORY);
            return client.get(client.getBatchPolicyDefault(),
                    pointerKeys.apply(context).toArray(new Key[0]),
                    AerospikeConstants.getFireHistoryBins());
        });
    }

    /** The newest checkpoint labelled at or before {@code instant}. */
    private Optional<Checkpoint> resolve(final MagazineContext context,
            final int shard,
            final List<Checkpoint> checkpoints,
            final Instant instant) {
        final long cutoff = instant.toEpochMilli();
        final Optional<Checkpoint> answer = checkpoints.stream()
                .filter(checkpoint -> checkpoint.label() <= cutoff)
                .findFirst();
        // At capacity with nothing old enough: the answer was evicted; fail rather than return empty.
        if (answer.isEmpty() && checkpoints.size() >= entries) {
            throw MagazineExceptions.invalidRequest(String.format(ErrorMessage.FIRE_HISTORY_TOO_SHORT,
                    context.getMagazineIdentifier(), shard, instant,
                    Instant.ofEpochMilli(checkpoints.get(checkpoints.size() - 1).label())));
        }
        return answer;
    }

    /** Checkpoints newest first, from either the full map or the refresh read's single entry. */
    private static List<Checkpoint> checkpoints(final Record pointerRecord) {
        final Object raw = Objects.isNull(pointerRecord) ? null : pointerRecord.getValue(AerospikeConstants.FIRE_HISTORY);
        final Stream<?> rawEntries;
        if (raw instanceof Map<?, ?> map) {
            rawEntries = map.entrySet().stream();
        } else if (raw instanceof List<?> list) {
            rawEntries = list.stream();
        } else {
            rawEntries = Stream.ofNullable(raw);
        }
        return rawEntries.map(Checkpoint::decode)
                .flatMap(Optional::stream)
                .sorted(Checkpoint.NEWEST_FIRST)
                .toList();
    }

    private void requireEnabled(final MagazineContext context) {
        if (!enabled) {
            throw MagazineExceptions.of(ErrorCode.NOT_ENABLED, String.format(
                    ErrorMessage.FIRE_HISTORY_DISABLED, context.getMagazineIdentifier()));
        }
    }

    private static long firePointer(final Record pointers) {
        return pointers.getLong(AerospikeConstants.FIRE_POINTER);
    }

    private static int capacityFor(final int expectedSize) {
        return (int) Math.ceil(expectedSize / 0.75d) + 1;
    }

    // --- value types -------------------------------------------------------------------------

    /** A recording window; {@code label} is its end. */
    private record Window(long start, long label) {

        static Window containing(final long now, final long width) {
            final long start = now / width * width;
            return new Window(start, start + width);
        }
    }

    /** By {@code label}, the fire pointer had reached at least {@code firePointer}. */
    private record Checkpoint(long label, long firePointer) {

        static final Comparator<Checkpoint> NEWEST_FIRST = Comparator.comparingLong(Checkpoint::label).reversed();

        /** Map keys are negated labels, so the key-ordered map sorts newest-first. */
        static long encode(final long label) {
            return -label;
        }

        static Optional<Checkpoint> decode(final Object raw) {
            if (raw instanceof Map.Entry<?, ?> entry
                    && entry.getKey() instanceof Number key
                    && entry.getValue() instanceof Number value) {
                return Optional.of(new Checkpoint(-key.longValue(), value.longValue()));
            }
            return Optional.empty();
        }

        FireCheckpoint toEntity() {
            return new FireCheckpoint(firePointer, Instant.ofEpochMilli(label));
        }
    }

    /** Per-process jitter and last claimed window per shard (released on failure for retry). */
    private static final class MagazineState {
        private final AtomicLong[] claimedWindows;
        @Getter
        private final long jitterMillis;

        MagazineState(final int shards, final long jitterMillis) {
            this.claimedWindows = new AtomicLong[shards];
            for (int shard = 0; shard < shards; shard++) {
                claimedWindows[shard] = new AtomicLong(Long.MIN_VALUE);
            }
            this.jitterMillis = jitterMillis;
        }

        boolean claim(final int shard, final Window window) {
            final AtomicLong claimed = claimedWindows[shard];
            final long previous = claimed.get();
            return previous < window.start() && claimed.compareAndSet(previous, window.start());
        }

        void release(final int shard, final Window window) {
            claimedWindows[shard].compareAndSet(window.start(), Long.MIN_VALUE);
        }
    }
}
