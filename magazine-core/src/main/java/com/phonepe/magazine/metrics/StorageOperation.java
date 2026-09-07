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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The storage call sites that issue a round trip.
 * <p>
 * A closed set rather than a free-form string, so meters can be resolved once per magazine and
 * indexed by ordinal instead of looked up by name on every call. It also keeps the
 * {@code operation} tag's cardinality bounded by construction.
 * <p>
 * Tag values are written out rather than derived from {@link #name()}, so the published metric
 * names are part of the source and renaming a constant cannot silently rename a series someone is
 * alerting on.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum StorageOperation {

    CLAIM_FIRE_POINTER("claim_fire_pointer"),
    INCREMENT_LOAD_POINTER("increment_load_pointer"),
    INCREMENT_LOAD_COUNTER("increment_load_counter"),
    INCREMENT_FIRE_COUNTER("increment_fire_counter"),
    DECREMENT_FIRE_COUNTER("decrement_fire_counter"),
    BATCH_READ_METADATA("batch_read_metadata"),
    /**
     * The batch read behind an active-shard cache refresh. Split from
     * {@link #BATCH_READ_METADATA} so refresh traffic is attributable without a meter of its own -
     * a magazine on the split metadata schema issues two of these per refresh, one per ledger.
     */
    REFRESH_ACTIVE_SHARDS("refresh_active_shards"),
    READ_DATA("read_data"),
    WRITE_DATA("write_data"),
    DELETE_DATA("delete_data"),
    BATCH_READ_DATA("batch_read_data"),
    CLAIM_DEDUPE_MARKER("claim_dedupe_marker"),
    WITHDRAW_DEDUPE_MARKER("withdraw_dedupe_marker");

    private final String tag;
}
