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

import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.impl.aerospike.store.DeDupeGuard;
import com.phonepe.magazine.metrics.MagazineMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Duplicate suppression, and that an unconfirmed claim is withdrawn so a failed load does not
 * suppress its own retry.
 */
class MagazineDeDupeTest extends AerospikeMagazineTestBase {

    @Test
    void dedupeSuppressesRepeatedLoad() {
        // The marker is claimed with a CREATE_ONLY write, so the server admits exactly one creator
        // and the second load is suppressed without any lock or read-before-write.
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("DEDUPE_CLASSPATH_MAGAZINE")
                .baseMagazineStorage(buildMagazineStorage(String.class, true))
                .build();

        assertTrue(magazine.load("SAME"));
        assertTrue(magazine.load("SAME"));
        assertEquals(1, collectMetaData(magazine.getMetaData()).getLoadCounter());
        assertEquals("SAME", magazine.fire().getData());
    }

    /**
     * The marker is written before the payload, so a load that never completes must withdraw it.
     * Otherwise a transient write failure would suppress every legitimate retry until the TTL
     * elapsed - silently dropping the caller's data.
     */
    @Test
    void unconfirmedDeDupeClaimIsWithdrawnSoRetriesAreNotSuppressed() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DeDupeGuard<String> guard = DeDupeGuard.aerospike(aerospikeClient,
                new MagazineMetrics(registry), "NAMESPACE", "FARM_ID", "CLIENT_ID",
                MagazineScope.LOCAL, 30 * 24 * 60 * 60);

        // First claim wins, but is abandoned without confirmation - as a failed load would.
        try (DeDupeGuard.Handle abandoned = guard.claim("WITHDRAWN_CLAIM_MAGAZINE", "PAYLOAD")) {
            assertFalse(abandoned.duplicate());
        }
        assertEquals(1.0, counterValue(registry, "magazine.aerospike.calls",
                "operation", "withdraw_dedupe_marker"));

        // Because the claim was withdrawn, a retry is admitted rather than suppressed.
        try (DeDupeGuard.Handle retry = guard.claim("WITHDRAWN_CLAIM_MAGAZINE", "PAYLOAD")) {
            assertFalse(retry.duplicate());
            retry.confirm();
        }

        // Now that it is confirmed the marker stands, and the payload is suppressed.
        try (DeDupeGuard.Handle suppressed = guard.claim("WITHDRAWN_CLAIM_MAGAZINE", "PAYLOAD")) {
            assertTrue(suppressed.duplicate());
        }
        assertEquals(2.0, counterValue(registry, "magazine.dedupe.outcomes", "outcome", "claimed"));
        assertEquals(1.0, counterValue(registry, "magazine.dedupe.outcomes", "outcome", "duplicate"));
    }

    @Test
    void dedupeDisabledAllowsRepeatedLoad() {
        AerospikeStorage<String> storage = buildMagazineStorage(String.class, false);
        Magazine<String> magazine = Magazine.<String>builder()
                .magazineIdentifier("MAGAZINE_WITHOUT_DEDUPE")
                .baseMagazineStorage(storage)
                .build();

        assertFalse(storage.isEnableDeDupe());
        assertTrue(magazine.load("DATA"));
        assertTrue(magazine.load("DATA"));
        assertEquals(2, collectMetaData(magazine.getMetaData()).getLoadCounter());
    }
}
