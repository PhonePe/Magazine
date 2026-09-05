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

import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AerospikeStorageConfig {

    private String namespace;
    private String metaSetName;
    private String dataSetName;
    @Builder.Default
    private int recordTtl = 30 * 24 * 60 * 60;  // 30 days default ttl
    /**
     * Shard count used when <em>creating</em> a magazine that does not yet exist. For an existing
     * magazine the persisted count is authoritative and this value is ignored, which is what lets
     * one storage instance serve magazines with differing shard counts.
     */
    @Builder.Default
    private int shards = 64; //Default 64 shards in a magazine
    /**
     * Permit widening an existing magazine's shard count to {@link #shards}. Off by default:
     * shard count is shared, persisted state and must not be mutated as a side effect of boot.
     * Narrowing is never possible, and an unsharded magazine can never be promoted to sharded.
     */
    @Builder.Default
    private boolean allowShardIncrease = false;
    @Builder.Default
    private int metaDataTtl = 2 * 30 * 24 * 60 * 60; // 2 months default TTL
    /**
     * How many times fire() retries after losing the fire-pointer claim, i.e. after making no
     * forward progress. Raise it for hot magazines with many concurrent consumers; exhausting it
     * yields {@code RETRIES_EXHAUSTED}, meaning "gave up under contention", not "queue empty".
     */
    @Builder.Default
    private int maxFireContentionAttempts = AerospikeConstants.MAX_FIRE_CONTENTION_ATTEMPTS;
    /**
     * How many consecutive pointer holes fire() skips before giving up. A hole is a slot whose
     * pointer was allocated but whose data write failed, so skipping one IS forward progress and
     * does not consume the contention budget. Raise it for magazines with a high write-failure rate.
     */
    @Builder.Default
    private int maxFireHoleSkips = AerospikeConstants.MAX_FIRE_HOLE_SKIPS;
}
