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

package com.phonepe.magazine.entity;

import java.time.Instant;

/**
 * Where a shard's fire pointer stood at a known moment.
 * <p>
 * The guarantee is one-directional and that is the whole point: every slot at or below
 * {@link #firePointer()} was claimed at or before {@link #recordedAt()}. It may under-report - the
 * pointer was possibly further along by then - but it can never over-report. A caller deciding
 * whether a record has been abandoned can therefore act on it without ever mistaking a live
 * delivery for a dead one.
 *
 * @param firePointer the highest slot known to have been claimed by {@code recordedAt}.
 * @param recordedAt  when that observation was taken. Exposed rather than hidden so callers can
 *                    tell how far behind the answer is and alert on it.
 */
public record FireCheckpoint(long firePointer, Instant recordedAt) {
}
