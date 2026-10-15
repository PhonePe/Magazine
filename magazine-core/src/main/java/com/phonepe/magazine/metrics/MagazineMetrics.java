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

package com.phonepe.magazine.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Instrumentation for the storage backends.
 * <p>
 * {@link #aerospikeCall} counts <em>round trips</em> rather than logical operations, which is the
 * only way to see a dequeue's cost change under contention.
 * <p>
 * Written to allocate nothing on the hot path: meters are resolved on first use into a
 * {@link Meters} holder indexed by enum ordinal, so recording is a map lookup plus an array index.
 * Percentile histograms are deliberately not enabled - they allocate hundreds of buckets per timer
 * and do bucket arithmetic per record. Add a {@link io.micrometer.core.instrument.config.MeterFilter}
 * for {@code magazine.*} on your own registry if you want them, where the cost is yours to choose.
 * <p>
 * There is no disabled mode. Switching instrumentation off is expressed as a registry with nothing
 * attached to it - an empty {@link io.micrometer.core.instrument.composite.CompositeMeterRegistry},
 * whose counters resolve to a no-op - so this class has one code path rather than a flag tested on
 * every call.
 */
public final class MagazineMetrics {

    public static final String AEROSPIKE_CALLS = "magazine.aerospike.calls";
    public static final String FIRE_CLAIMS = "magazine.fire.claims";
    /** Pointer slots skipped because the claim succeeded but the data record was absent. */
    public static final String FIRE_HOLE_SKIPS = "magazine.fire.hole.skips";
    public static final String FIRE_OUTCOMES = "magazine.fire.outcomes";
    public static final String LOAD_OUTCOMES = "magazine.load.outcomes";
    public static final String DEDUPE_OUTCOMES = "magazine.dedupe.outcomes";
    public static final String FIRE_LATENCY = "magazine.fire.latency";
    public static final String LOAD_LATENCY = "magazine.load.latency";

    private static final String TAG_MAGAZINE = "magazine";
    private static final String TAG_OUTCOME = "outcome";
    private static final String TAG_OPERATION = "operation";

    /** Lets {@link Meters#outcome} register any outcome enum without knowing which one it has. */
    interface Tagged {

        String getTag();
    }

    /**
     * Outcomes are split per meter rather than pooled into one enum, so a meter can only be given
     * a value it can actually report. Pooling them registered every outcome against every meter -
     * dozens of permanently-zero series per magazine, and nothing to stop a nonsensical pairing.
     */
    @Getter
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public enum ClaimOutcome implements Tagged {
        WON("won"),
        DRAINED("drained");

        private final String tag;
    }

    @Getter
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public enum FireOutcome implements Tagged {
        DELIVERED("delivered"),
        EMPTY("empty"),
        EXHAUSTED("exhausted"),
        FAILED("failed");

        private final String tag;
    }

    @Getter
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public enum LoadOutcome implements Tagged {
        LOADED("loaded"),
        DUPLICATE("duplicate"),
        FAILED("failed");

        private final String tag;
    }

    @Getter
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public enum DedupeOutcome implements Tagged {
        CLAIMED("claimed"),
        DUPLICATE("duplicate");

        private final String tag;
    }

    private final MeterRegistry registry;
    private final Map<String, Meters> byMagazine = new ConcurrentHashMap<>();

    public MagazineMetrics(final MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "Meter registry is required.");
    }

    public void aerospikeCall(final String magazineIdentifier, final StorageOperation operation) {
        meters(magazineIdentifier).call(operation).increment();
    }

    public void fireClaim(final String magazineIdentifier, final ClaimOutcome outcome) {
        final Meters meters = meters(magazineIdentifier);
        meters.outcome(meters.fireClaims, FIRE_CLAIMS, outcome).increment();
    }

    public void fireHoleSkip(final String magazineIdentifier) {
        meters(magazineIdentifier).fireHoleSkips().increment();
    }

    public void fireOutcome(final String magazineIdentifier, final FireOutcome outcome) {
        final Meters meters = meters(magazineIdentifier);
        meters.outcome(meters.fireOutcomes, FIRE_OUTCOMES, outcome).increment();
    }

    public void loadOutcome(final String magazineIdentifier, final LoadOutcome outcome) {
        final Meters meters = meters(magazineIdentifier);
        meters.outcome(meters.loadOutcomes, LOAD_OUTCOMES, outcome).increment();
    }

    public void dedupeOutcome(final String magazineIdentifier, final DedupeOutcome outcome) {
        final Meters meters = meters(magazineIdentifier);
        meters.outcome(meters.dedupeOutcomes, DEDUPE_OUTCOMES, outcome).increment();
    }

    /** A primitive rather than a {@code Timer.Sample}, so timing allocates nothing. */
    public long startNanos() {
        return System.nanoTime();
    }

    public void recordFire(final long startNanos, final String magazineIdentifier) {
        meters(magazineIdentifier).fireLatency()
                .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    public void recordLoad(final long startNanos, final String magazineIdentifier) {
        meters(magazineIdentifier).loadLatency()
                .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    private Meters meters(final String magazineIdentifier) {
        return byMagazine.computeIfAbsent(magazineIdentifier, id -> new Meters(registry, id));
    }

    /**
     * One magazine's meters, registered on first use. A racing first use resolves to the same
     * meter in the registry. No allocation once registered.
     */
    private static final class Meters {

        private final MeterRegistry registry;
        private final Tags magazine;
        private final Counter[] calls = new Counter[StorageOperation.values().length];
        private final Counter[] fireClaims = new Counter[ClaimOutcome.values().length];
        private final Counter[] fireOutcomes = new Counter[FireOutcome.values().length];
        private final Counter[] loadOutcomes = new Counter[LoadOutcome.values().length];
        private final Counter[] dedupeOutcomes = new Counter[DedupeOutcome.values().length];
        private Counter fireHoleSkips;
        private Timer fireLatency;
        private Timer loadLatency;

        private Meters(final MeterRegistry registry, final String magazineIdentifier) {
            this.registry = registry;
            this.magazine = Tags.of(TAG_MAGAZINE, magazineIdentifier);
        }

        Counter call(final StorageOperation operation) {
            Counter counter = calls[operation.ordinal()];
            if (Objects.isNull(counter)) {
                counter = Counter.builder(AEROSPIKE_CALLS)
                        .tags(magazine.and(TAG_OPERATION, operation.getTag()))
                        .register(registry);
                calls[operation.ordinal()] = counter;
            }
            return counter;
        }

        <E extends Enum<E> & Tagged> Counter outcome(final Counter[] slots, final String name, final E outcome) {
            Counter counter = slots[outcome.ordinal()];
            if (Objects.isNull(counter)) {
                counter = Counter.builder(name)
                        .tags(magazine.and(TAG_OUTCOME, outcome.getTag()))
                        .register(registry);
                slots[outcome.ordinal()] = counter;
            }
            return counter;
        }

        Counter fireHoleSkips() {
            if (Objects.isNull(fireHoleSkips)) {
                fireHoleSkips = Counter.builder(FIRE_HOLE_SKIPS).tags(magazine).register(registry);
            }
            return fireHoleSkips;
        }

        Timer fireLatency() {
            if (Objects.isNull(fireLatency)) {
                fireLatency = Timer.builder(FIRE_LATENCY).tags(magazine).register(registry);
            }
            return fireLatency;
        }

        Timer loadLatency() {
            if (Objects.isNull(loadLatency)) {
                loadLatency = Timer.builder(LOAD_LATENCY).tags(magazine).register(registry);
            }
            return loadLatency;
        }
    }
}
