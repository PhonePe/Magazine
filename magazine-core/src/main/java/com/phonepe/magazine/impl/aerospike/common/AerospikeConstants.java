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
    public static final String MAGAZINE_DISTRIBUTED_LOCK_SET_NAME_SUFFIX = "magazine_distributed_lock";
    public static final String DLM_CLIENT_ID = "magazine";
    /**
     * Deduplication keys its marker record on the payload's toString(), which is only stable and
     * collision-free for these value types.
     */
    public static final Set<Class<?>> DEDUPABLE_CLASSES = Set.of(String.class, Long.class, Integer.class);

    // --- retry / backoff --------------------------------------------------------------------
    public static final int MAX_RETRIES = 5;
    public static final long AEROSPIKE_RETRY_DELAY_MS = 10;
    /** Bounds fire() retries that made no forward progress (the pointer claim was lost). */
    public static final int MAX_FIRE_CONTENTION_ATTEMPTS = 16;
    /** Bounds consecutive pointer holes skipped by fire(); each skip IS forward progress. */
    public static final int MAX_FIRE_HOLE_SKIPS = 512;
    public static final long FIRE_BACKOFF_BASE_MS = 2;
    public static final long FIRE_BACKOFF_MAX_MS = 50;

    // --- active shard cache -----------------------------------------------------------------
    public static final int DEFAULT_REFRESH = 5;
    public static final int DEFAULT_MAX_ELEMENTS = 1024;

    public static final int SHARD_CONFIGURATION_TTL_SECONDS = 5 * 365 * 24 * 60 * 60;
}
