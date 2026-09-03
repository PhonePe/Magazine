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

package com.phonepe.magazine.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Set;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {
    public static final String DATA = "data";
    public static final String MODIFIED_AT = "modified_at";

    public static final String POINTERS = "POINTERS";
    public static final String METADATA = "METADATA";
    public static final String COUNTERS = "COUNTERS";

    public static final String LOAD_COUNTER = "LOAD_COUNTER";
    public static final String FIRE_COUNTER = "FIRE_COUNTER";
    public static final String LOAD_POINTER = "LOAD_POINTER";
    public static final String FIRE_POINTER = "FIRE_POINTER";
    public static final String METADATA_SCHEMA_VERSION = "META_VERSION";
    public static final String CREATED_AT = "CREATED_AT";

    public static final String KEY_DELIMITER = "_";

    public static final String SHARD_PREFIX = "SHARD";
    public static final String SHARDS_BIN = "SHARDS";
    public static final String MAGAZINE_DISTRIBUTED_LOCK_SET_NAME_SUFFIX = "magazine_distributed_lock";
    public static final String DLM_CLIENT_ID = "magazine";

    public static final int DEFAULT_REFRESH = 5;
    public static final int DEFAULT_MAX_ELEMENTS = 1024;
    public static final int MAX_RETRIES = 5;
    public static final int LEGACY_METADATA_SCHEMA_VERSION = 0;
    public static final int UNIFIED_METADATA_SCHEMA_VERSION = 2;
    public static final long AEROSPIKE_RETRY_DELAY_MS = 10;
    public static final int SHARD_CONFIGURATION_TTL_SECONDS = 5 * 365 * 24 * 60 * 60;

    public static final Set<Class<?>> DEDUPABLE_CLASSES = Set.of(
            String.class,
            Long.class,
            Integer.class
    );
}
