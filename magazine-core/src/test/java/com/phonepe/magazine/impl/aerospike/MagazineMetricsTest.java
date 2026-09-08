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

import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.entity.MagazineScope;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the round-trip cost of the hot paths. These numbers are the whole point of the claim
 * rewrite, and nothing else in the suite would notice a regression that reintroduced a read
 * before the claim.
 */
class MagazineMetricsTest extends AerospikeMagazineTestBase {

    /**
     * Pins the round-trip cost of the hot paths. These numbers are the whole point of the claim
     * rewrite, and nothing else in the suite would notice a regression that silently reintroduced
     * a read before the claim.
     */
    @Test
    void hotPathRoundTripCountsAreBounded() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AerospikeStorage<String> storage = buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET",
                        30 * 24 * 60 * 60, 2 * 30 * 24 * 60 * 60, 1),
                String.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient,
                registry);
        MagazineContext context = storage.initialize("ROUND_TRIP_MAGAZINE");

        assertTrue(storage.load(context, "PAYLOAD"));
        // allocate the pointer, write the payload, publish the counter
        assertEquals(1.0, counterValue(registry, "magazine.aerospike.calls",
                "operation", "increment_load_pointer"));
        assertEquals(1.0, counterValue(registry, "magazine.aerospike.calls", "operation", "write_data"));
        assertEquals(1.0, counterValue(registry, "magazine.aerospike.calls",
                "operation", "increment_load_counter"));

        assertEquals("PAYLOAD", storage.fire(context).getData());
        // claim the pointer, read the payload, publish the counter - and crucially NO separate
        // pointer read, which is what used to make the claim lose under concurrency.
        assertEquals(1.0, counterValue(registry, "magazine.aerospike.calls",
                "operation", "claim_fire_pointer"));
        assertEquals(1.0, counterValue(registry, "magazine.aerospike.calls", "operation", "read_data"));
        assertEquals(1.0, counterValue(registry, "magazine.aerospike.calls",
                "operation", "increment_fire_counter"));
        // exactly one claim, and it was won - no contention retries
        assertEquals(1.0, counterValue(registry, "magazine.fire.claims", "outcome", "won"));
        assertEquals(1.0, counterValue(registry, "magazine.fire.outcomes", "outcome", "delivered"));
    }

    @Test
    void metricsCanBeDisabledExplicitlyEvenWithARegistry() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AerospikeStorage<String> storage = buildStorage(
                AerospikeStorageConfig.builder()
                        .namespace("NAMESPACE").dataSetName("DATA_SET").metaSetName("META_SET")
                        .recordTtl(30 * 24 * 60 * 60).metaDataTtl(2 * 30 * 24 * 60 * 60)
                        .shards(1).metricsEnabled(false).build(),
                String.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL,
                aerospikeClient, registry);
        MagazineContext context = storage.initialize("METRICS_DISABLED_MAGAZINE");

        assertTrue(storage.load(context, "PAYLOAD"));

        assertEquals(0.0, counterValue(registry, "magazine.aerospike.calls", "operation", "write_data"));
        assertTrue(registry.getMeters().isEmpty(), "disabled metrics must register no meters");
    }
}
