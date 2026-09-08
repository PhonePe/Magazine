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

package com.phonepe.magazine.impl.aerospike.store;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.exception.MagazineExceptions;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikeNaming;
import com.phonepe.magazine.impl.aerospike.common.AerospikePolicies;
import com.phonepe.magazine.impl.aerospike.common.AerospikeRetryer;
import com.phonepe.magazine.impl.aerospike.common.ErrorMessage;
import com.phonepe.magazine.metrics.MagazineMetrics;
import com.phonepe.magazine.metrics.StorageOperation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reads and writes the per-shard metadata records: load/fire pointers and load/fire counters.
 * <p>
 * Pointers and counters live in one record under the unified schema and in two under the legacy
 * schema; that difference is confined to {@link AerospikeNaming}, so every method here is written
 * once and works for both.
 *
 * The filter expression, the write policies and the metadata keys are all constant per magazine
 * and all resolved once: {@link Exp#build} serialises to a byte array, a policy is a ~25-field
 * copy, and {@link Key} construction runs a RIPEMD-160 digest. Rebuilding those per operation was
 * pure overhead on every round trip.
 */
public final class MagazineMetadataStore {

    /**
     * The claim's precondition: this shard still has an unconsumed slot. Absent bins read as zero,
     * so a record never fired from does not make the comparison unknown.
     */
    private static final com.aerospike.client.exp.Expression FIRE_POINTER_BEHIND_LOAD_POINTER =
            Exp.build(Exp.lt(
                    binOrZero(AerospikeConstants.FIRE_POINTER),
                    binOrZero(AerospikeConstants.LOAD_POINTER)));

    private final IAerospikeClient client;
    private final AerospikeRetryer retryerFactory;
    private final MagazineMetrics metrics;
    private final String namespace;
    private final String metaSetName;

    private final WritePolicy claimPolicy;
    private final WritePolicy adjustPolicy;
    /** Bounded by the number of configured magazines. */
    private final Map<MagazineContext, MetadataKeys> keyCache = new ConcurrentHashMap<>();

    public MagazineMetadataStore(final IAerospikeClient client,
            final AerospikeRetryer retryerFactory,
            final MagazineMetrics metrics,
            final String namespace,
            final String metaSetName,
            final int metaDataTtl) {
        this.client = client;
        this.retryerFactory = retryerFactory;
        this.metrics = metrics;
        this.namespace = namespace;
        this.metaSetName = metaSetName;

        this.claimPolicy = AerospikePolicies.writePolicy(client);
        this.claimPolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        this.claimPolicy.maxRetries = 0;
        this.claimPolicy.failOnFilteredOut = true;
        this.claimPolicy.expiration = metaDataTtl;
        this.claimPolicy.filterExp = FIRE_POINTER_BEHIND_LOAD_POINTER;

        this.adjustPolicy = AerospikePolicies.writePolicy(client);
        this.adjustPolicy.recordExistsAction = RecordExistsAction.UPDATE;
        this.adjustPolicy.expiration = metaDataTtl;
    }

    /**
     * Atomically claims the next fire pointer on a shard.
     * <p>
     * The claim is a single guarded increment: the filter expression asserts
     * {@code FIRE_POINTER < LOAD_POINTER} against the pre-write record, and the increment returns
     * the value it produced. Because every caller increments rather than compare-and-swapping an
     * expected value, <em>distinct callers always receive distinct pointers</em>. There is no
     * such thing as a lost claim, so no contention budget, no backoff and no thundering herd -
     * the previous read-then-CAS made every concurrent consumer read the same pointer and all but
     * one fail, which made the call count per dequeue scale with consumer count.
     * <p>
     * A filtered-out claim means the shard is drained, which is the caller's signal to suppress it.
     * <p>
     * Deliberately <em>not</em> retried: {@link Operation#add} is not idempotent, so retrying
     * after a timeout could double-advance the pointer and drop a record. The cost is that a
     * timeout the server did apply silently skips one record - the at-most-once boundary
     * documented on {@link com.phonepe.magazine.Magazine#fire()}.
     *
     * @return the claimed pointer, or empty when the shard has nothing left to fire.
     */
    public OptionalLong claimFirePointer(final MagazineContext context, final Integer shard) {
        metrics.aerospikeCall(context.getMagazineIdentifier(), StorageOperation.CLAIM_FIRE_POINTER);
        try {
            final Record claimResult = client.operate(claimPolicy, pointerKey(context, shard),
                    Operation.add(new Bin(AerospikeConstants.FIRE_POINTER, 1L)),
                    Operation.get(AerospikeConstants.FIRE_POINTER));
            return OptionalLong.of(claimResult.getLong(AerospikeConstants.FIRE_POINTER));
        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.FILTERED_OUT) {
                return OptionalLong.empty();
            }
            if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                throw missingMetadata(context, shard);
            }
            throw e;
        }
    }

    public long incrementAndGetLoadPointer(final MagazineContext context, final Integer shard) {
        final Record updated = operateOnMetadata(pointerKey(context, shard), context,
                StorageOperation.INCREMENT_LOAD_POINTER, AerospikeConstants.LOAD_POINTER, 1L);
        if (Objects.isNull(updated)) {
            throw MagazineExceptions.magazineUnprepared(
                    String.format(ErrorMessage.ERROR_READING_POINTERS, context.getMagazineIdentifier()));
        }
        return updated.getLong(AerospikeConstants.LOAD_POINTER);
    }

    public void incrementLoadCounter(final MagazineContext context, final Integer shard) {
        adjustCounter(context, shard, StorageOperation.INCREMENT_LOAD_COUNTER, AerospikeConstants.LOAD_COUNTER, 1L);
    }

    /**
     * Publishes a delivery in the counter ledger.
     * <p>
     * This can no longer ride along with the pointer claim, because the claim now happens
     * <em>before</em> the data record is read - so at claim time we do not yet know whether the
     * slot holds real data or is a hole, and only real deliveries may be counted. A crash between
     * the claim and this call therefore leaves {@code FIRE_COUNTER} short, which inflates the
     * reported pending depth and keeps the shard active a little longer. That is the safe
     * direction: it can never hide a shard that still holds data.
     */
    public void incrementFireCounter(final MagazineContext context, final Integer shard) {
        adjustCounter(context, shard, StorageOperation.INCREMENT_FIRE_COUNTER, AerospikeConstants.FIRE_COUNTER, 1L);
    }

    public void decrementFireCounter(final MagazineContext context, final Integer shard) {
        adjustCounter(context, shard, StorageOperation.DECREMENT_FIRE_COUNTER, AerospikeConstants.FIRE_COUNTER, -1L);
    }

    public Map<String, MetaData> readAll(final MagazineContext context) {
        final Record[] pointerRecords = batchRead(context, keys(context).pointerKeys(),
                StorageOperation.BATCH_READ_METADATA);
        final Record[] counterRecords = AerospikeNaming.usesUnifiedMetadata(context)
                ? pointerRecords
                : batchRead(context, keys(context).counterKeys(), StorageOperation.BATCH_READ_METADATA);

        final Map<String, MetaData> metaData = new HashMap<>(capacityFor(context.getShards()));
        for (int shard = 0; shard < context.getShards(); shard++) {
            final Record pointers = pointerRecords[shard];
            final Record counters = counterRecords[shard];
            metaData.put(context.shardId(shard), new MetaData(
                    longOrZero(counters, AerospikeConstants.FIRE_COUNTER),
                    longOrZero(counters, AerospikeConstants.LOAD_COUNTER),
                    longOrZero(pointers, AerospikeConstants.FIRE_POINTER),
                    longOrZero(pointers, AerospikeConstants.LOAD_POINTER)));
        }
        return metaData;
    }

    /**
     * A shard is active only when <em>both</em> the pointer and the counter ledgers say so.
     * <p>
     * {@code LOAD_POINTER} counts slots allocated; {@code LOAD_COUNTER} counts slots confirmed
     * written. They diverge by the number of holes, because a load burns a pointer before the data
     * write and only publishes the counter once that write succeeds. So the pointer clause answers
     * "are there unconsumed slots?" and the counter clause answers "does any of it hold real data?".
     * <p>
     * Both are required. Dropping the counter clause would make fire() spin through its hole-skip
     * budget on shards containing nothing but failed writes. The ledger is sound because
     * {@code FIRE_COUNTER} can never over-count: it advances only after a data record was actually
     * found, so the worst a crash can do is leave it short, which keeps the shard active and
     * therefore fails open.
     *
     * @return shard numbers with data available to fire, ascending.
     */
    public int[] activeShards(final MagazineContext context) {
        final Record[] pointerRecords = batchRead(context, keys(context).pointerKeys(),
                StorageOperation.REFRESH_ACTIVE_SHARDS);
        final Record[] counterRecords = AerospikeNaming.usesUnifiedMetadata(context)
                ? pointerRecords
                : batchRead(context, keys(context).counterKeys(), StorageOperation.REFRESH_ACTIVE_SHARDS);

        final int[] active = new int[context.getShards()];
        int found = 0;
        for (int shard = 0; shard < context.getShards(); shard++) {
            final Record pointers = pointerRecords[shard];
            final Record counters = counterRecords[shard];
            if (Objects.nonNull(pointers)
                    && Objects.nonNull(counters)
                    && counters.getLong(AerospikeConstants.LOAD_COUNTER) > counters.getLong(AerospikeConstants.FIRE_COUNTER)
                    && pointers.getLong(AerospikeConstants.LOAD_POINTER) > pointers.getLong(AerospikeConstants.FIRE_POINTER)) {
                active[found++] = shard;
            }
        }
        return found == active.length ? active : java.util.Arrays.copyOf(active, found);
    }

    /**
     * The metadata TTL is validated to outlive the record TTL, so a missing metadata record means
     * the data is gone too. Surface it rather than quietly hiding the shard.
     */
    public static com.phonepe.magazine.exception.MagazineException missingMetadata(final MagazineContext context,
            final Integer shard) {
        return MagazineExceptions.magazineUnprepared(String.format(ErrorMessage.MISSING_METADATA_RECORD,
                context.getMagazineIdentifier(), Objects.isNull(shard) ? 0 : shard));
    }

    private void adjustCounter(final MagazineContext context,
            final Integer shard,
            final StorageOperation operation,
            final String counterBin,
            final long delta) {
        if (Objects.isNull(operateOnMetadata(counterKey(context, shard), context, operation,
                counterBin, delta))) {
            throw MagazineExceptions.magazineUnprepared(
                    String.format(ErrorMessage.ERROR_READING_COUNTERS, context.getMagazineIdentifier()));
        }
    }

    private Record operateOnMetadata(final Key key,
            final MagazineContext context,
            final StorageOperation operation,
            final String bin,
            final long delta) {
        return retryerFactory.call(() -> {
            metrics.aerospikeCall(context.getMagazineIdentifier(), operation);
            return client.operate(adjustPolicy, key,
                    Operation.add(new Bin(bin, delta)),
                    Operation.get(bin));
        });
    }

    private Record[] batchRead(final MagazineContext context,
            final List<Key> keys,
            final StorageOperation operation) {
        return retryerFactory.call(() -> {
            metrics.aerospikeCall(context.getMagazineIdentifier(), operation);
            return client.get(client.getBatchPolicyDefault(), keys.toArray(new Key[0]),
                    AerospikeConstants.getMetadataBins());
        });
    }

    private Key pointerKey(final MagazineContext context, final Integer shard) {
        return keys(context).pointerKeys().get(Objects.isNull(shard) ? 0 : shard);
    }

    private Key counterKey(final MagazineContext context, final Integer shard) {
        return keys(context).counterKeys().get(Objects.isNull(shard) ? 0 : shard);
    }

    private MetadataKeys keys(final MagazineContext context) {
        return keyCache.computeIfAbsent(context, this::buildKeys);
    }

    private MetadataKeys buildKeys(final MagazineContext context) {
        final List<Key> pointerKeys = List.of(AerospikeNaming.metaKeys(namespace, metaSetName, context,
                AerospikeNaming.pointerSuffix(context)));
        // Unified magazines co-locate counters in the pointer record, so the lists are the same.
        final List<Key> counterKeys = AerospikeNaming.usesUnifiedMetadata(context)
                ? pointerKeys
                : List.of(AerospikeNaming.metaKeys(namespace, metaSetName, context, AerospikeConstants.COUNTERS));
        return new MetadataKeys(pointerKeys, counterKeys);
    }

    /** Reads an integer bin as zero when the bin is absent, so comparisons never go unknown. */
    private static Exp binOrZero(final String bin) {
        return Exp.cond(Exp.binExists(bin), Exp.intBin(bin), Exp.val(0L));
    }

    private static long longOrZero(final Record source, final String bin) {
        return Objects.nonNull(source) ? source.getLong(bin) : 0L;
    }

    private record MetadataKeys(List<Key> pointerKeys, List<Key> counterKeys) {
    }

    /** Initial capacity that avoids a rehash for the given element count at the default 0.75f. */
    private static int capacityFor(final int expectedSize) {
        return (int) Math.ceil(expectedSize / 0.75d) + 1;
    }
}
