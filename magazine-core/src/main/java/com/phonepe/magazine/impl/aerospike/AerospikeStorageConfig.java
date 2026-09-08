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
    private int recordTtl = 30 * 24 * 60 * 60;  // 30 days
    /**
     * Shard count used only when <em>creating</em> a magazine. For an existing magazine the
     * persisted count is authoritative and this is ignored, which lets one storage instance serve
     * magazines with differing shard counts.
     * <p>
     * Shards spread records across partitions and keep any one metadata record from becoming a hot
     * key. They no longer dilute fire-pointer contention, so the default is modest: every extra
     * shard widens the active-shard batch read.
     */
    @Builder.Default
    private int shards = AerospikeConstants.DEFAULT_SHARDS;
    /**
     * Permit widening an existing magazine's shard count to {@link #shards}. Off by default:
     * shard count is shared, persisted state and must not be mutated as a side effect of boot.
     * Narrowing is never possible, and an unsharded magazine can never be promoted to sharded.
     */
    @Builder.Default
    private boolean allowShardIncrease = false;
    @Builder.Default
    private int metaDataTtl = 2 * 30 * 24 * 60 * 60; // 2 months
    /**
     * Seconds between refreshes of the cached active-shard set, and the primary lever on
     * steady-state read load: roughly {@code shards / activeShardRefreshSeconds} key reads per
     * second per magazine under consumption, nothing while idle. Raising it only widens the window
     * for firing at a drained shard, which is harmless - the claim reports it drained.
     */
    @Builder.Default
    private int activeShardRefreshSeconds = AerospikeConstants.DEFAULT_REFRESH;
    /**
     * How many consecutive pointer holes fire() skips before giving up. A hole is a slot whose
     * pointer was allocated but whose data write failed. Exhausting it yields
     * {@code RETRIES_EXHAUSTED} - "gave up", not "queue empty". There is no companion contention
     * budget: the claim is a guarded atomic increment and can never be lost.
     */
    @Builder.Default
    private int maxFireHoleSkips = AerospikeConstants.MAX_FIRE_HOLE_SKIPS;
    /**
     * Whether to record metrics. Meters go to the {@code meterRegistry} on the storage builder, or
     * to Micrometer's global registry when none was given - so the common case needs no wiring.
     * <p>
     * Setting this false publishes to a private empty registry instead, which means the opt-out
     * cannot be silently undone by another component attaching a registry globally.
     */
    @Builder.Default
    private boolean metricsEnabled = true;
}
