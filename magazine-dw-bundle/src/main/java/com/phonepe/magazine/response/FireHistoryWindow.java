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


package com.phonepe.magazine.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;

/**
 * One checkpoint window, already reduced to numbers.
 * <p>
 * Reduced server-side rather than shipped as a shard-to-pointer map because shard count is
 * unbounded: a 256-shard magazine at the maximum 128 retained windows would otherwise be a 32,768
 * entry payload on a 30-second poll, and a 256-column table nobody can read. A window row is
 * O(1) in shard count, so this scales to any magazine; one shard at a time is available by asking
 * for it.
 *
 * @param shardsReporting shards that recorded this window. Below the configured count is normal -
 *                        a shard has no metadata record until something is loaded to it.
 * @param firePointer     summed across reporting shards, or that shard's own pointer when one
 *                        shard was asked for.
 * @param delivered       slots handed out since the next-older window, or null for the oldest row.
 *                        Summed only over shards present in <em>both</em> windows: a shard that
 *                        appeared in between would otherwise show its whole pointer as a delta.
 */
public record FireHistoryWindow(@JsonFormat(shape = JsonFormat.Shape.STRING) Instant recordedAt,
                                long ageSeconds,
                                int shardsReporting,
                                long firePointer,
                                Long delivered) {
}
