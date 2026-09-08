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
 * Written to allocate nothing on the hot path: meters are resolved once per magazine into a
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

    /** Lets {@link Meters#outcomes} register any outcome enum without knowing which one it has. */
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
        meters(magazineIdentifier).calls[operation.ordinal()].increment();
    }

    public void fireClaim(final String magazineIdentifier, final ClaimOutcome outcome) {
        meters(magazineIdentifier).fireClaims[outcome.ordinal()].increment();
    }

    public void fireHoleSkip(final String magazineIdentifier) {
        meters(magazineIdentifier).fireHoleSkips.increment();
    }

    public void fireOutcome(final String magazineIdentifier, final FireOutcome outcome) {
        meters(magazineIdentifier).fireOutcomes[outcome.ordinal()].increment();
    }

    public void loadOutcome(final String magazineIdentifier, final LoadOutcome outcome) {
        meters(magazineIdentifier).loadOutcomes[outcome.ordinal()].increment();
    }

    public void dedupeOutcome(final String magazineIdentifier, final DedupeOutcome outcome) {
        meters(magazineIdentifier).dedupeOutcomes[outcome.ordinal()].increment();
    }

    /** A primitive rather than a {@code Timer.Sample}, so timing allocates nothing. */
    public long startNanos() {
        return System.nanoTime();
    }

    public void recordFire(final long startNanos, final String magazineIdentifier) {
        meters(magazineIdentifier).fireLatency
                .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    public void recordLoad(final long startNanos, final String magazineIdentifier) {
        meters(magazineIdentifier).loadLatency
                .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    private Meters meters(final String magazineIdentifier) {
        return byMagazine.computeIfAbsent(magazineIdentifier, id -> new Meters(registry, id));
    }

    private static final class Meters {

        private final Counter[] calls;
        private final Counter[] fireClaims;
        private final Counter[] fireOutcomes;
        private final Counter[] loadOutcomes;
        private final Counter[] dedupeOutcomes;
        private final Counter fireHoleSkips;
        private final Timer fireLatency;
        private final Timer loadLatency;

        private Meters(final MeterRegistry registry, final String magazineIdentifier) {
            final Tags magazine = Tags.of(TAG_MAGAZINE, magazineIdentifier);
            final StorageOperation[] operations = StorageOperation.values();
            this.calls = new Counter[operations.length];
            for (StorageOperation operation : operations) {
                this.calls[operation.ordinal()] = Counter.builder(AEROSPIKE_CALLS)
                        .tags(magazine.and(TAG_OPERATION, operation.getTag()))
                        .register(registry);
            }
            this.fireClaims = outcomes(registry, FIRE_CLAIMS, magazine, ClaimOutcome.values());
            this.fireOutcomes = outcomes(registry, FIRE_OUTCOMES, magazine, FireOutcome.values());
            this.loadOutcomes = outcomes(registry, LOAD_OUTCOMES, magazine, LoadOutcome.values());
            this.dedupeOutcomes = outcomes(registry, DEDUPE_OUTCOMES, magazine, DedupeOutcome.values());
            this.fireHoleSkips = Counter.builder(FIRE_HOLE_SKIPS).tags(magazine).register(registry);
            this.fireLatency = Timer.builder(FIRE_LATENCY).tags(magazine).register(registry);
            this.loadLatency = Timer.builder(LOAD_LATENCY).tags(magazine).register(registry);
        }

        private static <E extends Enum<E> & Tagged> Counter[] outcomes(final MeterRegistry registry,
                final String name,
                final Tags magazine,
                final E[] values) {
            final Counter[] counters = new Counter[values.length];
            for (E outcome : values) {
                counters[outcome.ordinal()] = Counter.builder(name)
                        .tags(magazine.and(TAG_OUTCOME, outcome.getTag()))
                        .register(registry);
            }
            return counters;
        }
    }
}
