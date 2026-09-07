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

package com.phonepe.magazine.impl.aerospike.common;

import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Aerospike bin names, key fragments and tuning knobs.
 * <p>
 * Package-private on purpose: these describe the on-disk layout of the Aerospike backend. Exposing
 * them publicly - as the previous {@code common.Constants} did - would freeze bin names as API on
 * the first release and let callers depend on storage internals.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AerospikeConstants {

    // --- bins -------------------------------------------------------------------------------
    public static final String DATA = "data";
    public static final String MODIFIED_AT = "modified_at";
    public static final String LOAD_COUNTER = "LOAD_COUNTER";
    public static final String FIRE_COUNTER = "FIRE_COUNTER";
    public static final String LOAD_POINTER = "LOAD_POINTER";
    public static final String FIRE_POINTER = "FIRE_POINTER";
    public static final String METADATA_SCHEMA_VERSION = "META_VERSION";
    public static final String CREATED_AT = "CREATED_AT";
    public static final String SHARDS_BIN = "SHARDS";

    /**
     * Bins to project on metadata batch reads. Metadata records are read {@code shards}-wide on
     * every active-shard refresh, so projecting keeps that fan-out from carrying bins nobody reads.
     */
    public static final String[] METADATA_BINS = {
            LOAD_POINTER, FIRE_POINTER, LOAD_COUNTER, FIRE_COUNTER};

    /** Bins to project on data reads - the payload is the only bin any caller consumes. */
    public static final String[] DATA_BINS = {DATA};

    // --- key suffixes -----------------------------------------------------------------------
    public static final String KEY_DELIMITER = "_";
    /** Unified schema: pointers and counters share one record. */
    public static final String METADATA = "METADATA";
    /** Legacy schema: pointers and counters live in separate records. */
    public static final String POINTERS = "POINTERS";
    public static final String COUNTERS = "COUNTERS";

    // --- metadata schema versions -----------------------------------------------------------
    public static final int LEGACY_METADATA_SCHEMA_VERSION = 0;
    public static final int UNIFIED_METADATA_SCHEMA_VERSION = 2;

    // --- deduplication ----------------------------------------------------------------------
    /**
     * Deduplication keys its marker record on the payload's toString(), which is only stable and
     * collision-free for these value types.
     */
    public static final Set<Class<?>> DEDUPABLE_CLASSES = Set.of(String.class, Long.class, Integer.class);

    // --- retry / backoff --------------------------------------------------------------------
    public static final int MAX_RETRIES = 5;
    public static final long AEROSPIKE_RETRY_DELAY_MS = 10;
    /**
     * Bounds consecutive pointer holes skipped by fire(); each skip IS forward progress.
     * <p>
     * There is deliberately no companion contention budget. The fire pointer is claimed by a
     * single guarded atomic increment, so distinct callers always receive distinct pointers and
     * a claim can never be lost to a competing consumer.
     */
    public static final int MAX_FIRE_HOLE_SKIPS = 512;

    // --- active shard cache -----------------------------------------------------------------
    /**
     * Seconds between active-shard cache refreshes. Each refresh is a batch read fanning out to
     * every shard of the magazine, so the steady-state discovery cost is
     * {@code magazines x shards / refresh} key reads per second.
     */
    public static final int DEFAULT_REFRESH = 5;
    public static final int DEFAULT_MAX_ELEMENTS = 1024;

    /**
     * Shard count applied when creating a magazine that does not yet exist.
     * <p>
     * Shards exist to spread load across Aerospike partitions and to keep any single metadata
     * record from becoming a hot key. They no longer serve to dilute fire-pointer contention -
     * the guarded atomic claim removed that - so this default was reduced from 64, which made
     * active-shard discovery eight times more expensive than the hot-key protection required.
     */
    public static final int DEFAULT_SHARDS = 8;

    public static final int SHARD_CONFIGURATION_TTL_SECONDS = 5 * 365 * 24 * 60 * 60;
}
