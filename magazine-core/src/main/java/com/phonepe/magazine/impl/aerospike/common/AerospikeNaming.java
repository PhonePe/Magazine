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

import com.aerospike.client.Key;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.entity.MagazineScope;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Every name this backend derives - record keys, set names and lock levels - in one place.
 * <p>
 * Key layout:
 * <pre>
 *   sharded:   &lt;magazine&gt;_SHARD_&lt;n&gt;_&lt;suffix&gt;
 *   unsharded: &lt;magazine&gt;_&lt;suffix&gt;
 * </pre>
 * The suffix is either a pointer value (data records) or a metadata discriminator - {@code METADATA}
 * for the unified schema, {@code POINTERS}/{@code COUNTERS} for the legacy split schema. Unsharded
 * keys carry no shard fragment at all, which is why an unsharded magazine can never be promoted to
 * a sharded one without orphaning its records.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AerospikeNaming {

    private static final String LOCAL_SCOPE_SET_FORMAT = "%s_%s";
    private static final String DEDUPER_SET_FORMAT = "%s_deduper";

    /** Key name for a record belonging to {@code shard} - pass null for an unsharded magazine. */
    public static String name(final MagazineContext context, final Integer shard, final String suffix) {
        return Objects.nonNull(shard) && !context.isUnsharded()
                ? context.getMagazineIdentifier() + AerospikeConstants.KEY_DELIMITER + context.shardId(shard)
                        + AerospikeConstants.KEY_DELIMITER + suffix
                : context.getMagazineIdentifier() + AerospikeConstants.KEY_DELIMITER + suffix;
    }

    /**
     * @return true when this magazine keeps counters and pointers in a single record, letting the
     *         fire pointer and fire counter advance atomically.
     */
    public static boolean usesUnifiedMetadata(final MagazineContext context) {
        return context.getStorageSchemaVersion() == AerospikeConstants.UNIFIED_METADATA_SCHEMA_VERSION;
    }

    /** Suffix holding the pointers - unified magazines co-locate counters in the same record. */
    public static String pointerSuffix(final MagazineContext context) {
        return usesUnifiedMetadata(context) ? AerospikeConstants.METADATA : AerospikeConstants.POINTERS;
    }

    /** Suffix holding the counters - unified magazines co-locate pointers in the same record. */
    public static String counterSuffix(final MagazineContext context) {
        return usesUnifiedMetadata(context) ? AerospikeConstants.METADATA : AerospikeConstants.COUNTERS;
    }

    public static String shardConfigurationName(final String magazineIdentifier) {
        return magazineIdentifier + AerospikeConstants.KEY_DELIMITER + AerospikeConstants.SHARDS_BIN;
    }

    /** One key per shard, ordered by shard number, for batch metadata reads. */
    public static Key[] metaKeys(final String namespace,
            final String metaSetName,
            final MagazineContext context,
            final String suffix) {
        if (context.isUnsharded()) {
            return new Key[]{new Key(namespace, metaSetName, name(context, null, suffix))};
        }
        final Key[] keys = new Key[context.getShards()];
        for (int shard = 0; shard < context.getShards(); shard++) {
            keys[shard] = new Key(namespace, metaSetName, name(context, shard, suffix));
        }
        return keys;
    }

    /** Local-scoped magazines are farm-prefixed; global-scoped ones share a set across farms. */
    public static String resolveSetName(final String setName,
            final String farmId,
            final MagazineScope scope) {
        return scope.accept(new MagazineScope.Visitor<>() {
            @Override
            public String visitLocal() {
                return String.format(LOCAL_SCOPE_SET_FORMAT, farmId, setName);
            }

            @Override
            public String visitGlobal() {
                return setName;
            }
        });
    }

    public static String deDuperSetName(final String clientId) {
        return DEDUPER_SET_FORMAT.formatted(clientId);
    }
}
