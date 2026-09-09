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

import java.util.List;

/**
 * @param enabled false when the magazine does not record delivery-time checkpoints. Reported rather
 *                than raised: a disabled feature is a configuration fact, not a failure.
 * @param shards  every shard with recorded history, for choosing one to drill into.
 * @param shard   the shard {@code windows} describes, or null when they are totals across shards.
 * @param span    how far back history reaches, or null when nothing is recorded yet.
 * @param windows retained windows, newest first. Bounded by {@code fireHistoryEntries}, and
 *                independent of shard count.
 */
public record FireHistoryResponse(String identifier,
                                  boolean enabled,
                                  List<String> shards,
                                  String shard,
                                  FireHistorySpan span,
                                  List<FireHistoryWindow> windows) {
}
