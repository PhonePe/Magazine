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
import com.github.rholder.retry.RetryException;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.exception.MagazineExceptions;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikeNaming;
import com.phonepe.magazine.impl.aerospike.common.AerospikeRetryerFactory;
import com.phonepe.magazine.impl.aerospike.common.ErrorMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;

/**
 * Reads and writes the per-shard metadata records: load/fire pointers and load/fire counters.
 * <p>
 * Pointers and counters live in one record under the unified schema and in two under the legacy
 * schema; that difference is confined to {@link AerospikeNaming}, so every method here is written
 * once and works for both.
 */
@RequiredArgsConstructor(access = lombok.AccessLevel.PUBLIC)
public final class MagazineMetadataStore {

    private final IAerospikeClient client;
    private final AerospikeRetryerFactory retryerFactory;
    private final String namespace;
    private final String metaSetName;
    private final int metaDataTtl;

    public Record readPointers(final MagazineContext context, final Integer shard)
            throws ExecutionException, RetryException {
        final Key key = pointerKey(context, shard);
        return retryerFactory.call(() -> client.get(client.getReadPolicyDefault(), key));
    }

    /**
     * Atomically advances {@code FIRE_POINTER} if and only if it still holds
     * {@code expectedFirePointer}, claiming that pointer for exactly one caller. Under the unified
     * schema {@code FIRE_COUNTER} advances in the same operation when a data record was found, so
     * pointer and counter can never disagree.
     * <p>
     * Deliberately <em>not</em> retried: {@link Operation#add} is not idempotent, so retrying after
     * a timeout could double-advance the pointer and drop a record. The cost is that a timeout the
     * server did apply silently skips one record - the at-most-once boundary documented on
     * {@link com.phonepe.magazine.Magazine#fire()}.
     *
     * @return true if this caller won the pointer, false if another caller got there first.
     */
    public boolean claimFirePointer(final MagazineContext context,
            final Integer shard,
            final long expectedFirePointer,
            final boolean incrementCounter) {
        final WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        writePolicy.maxRetries = 0;
        writePolicy.failOnFilteredOut = true;
        writePolicy.expiration = metaDataTtl;
        // A fresh record may not carry the bin at all, so treat "absent" and "zero" alike.
        writePolicy.filterExp = Exp.build(expectedFirePointer == 0L
                ? Exp.or(
                        Exp.not(Exp.binExists(AerospikeConstants.FIRE_POINTER)),
                        Exp.eq(Exp.intBin(AerospikeConstants.FIRE_POINTER), Exp.val(0L)))
                : Exp.eq(Exp.intBin(AerospikeConstants.FIRE_POINTER), Exp.val(expectedFirePointer)));

        try {
            final Key key = pointerKey(context, shard);
            if (incrementCounter && AerospikeNaming.usesUnifiedMetadata(context)) {
                client.operate(writePolicy, key,
                        Operation.add(new Bin(AerospikeConstants.FIRE_POINTER, 1L)),
                        Operation.add(new Bin(AerospikeConstants.FIRE_COUNTER, 1L)));
            } else {
                client.operate(writePolicy, key,
                        Operation.add(new Bin(AerospikeConstants.FIRE_POINTER, 1L)));
            }
            return true;
        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.FILTERED_OUT) {
                return false;
            }
            if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                throw missingMetadata(context, shard);
            }
            throw e;
        }
    }

    public long incrementAndGetLoadPointer(final MagazineContext context, final Integer shard)
            throws ExecutionException, RetryException {
        final Record record = operateOnMetadata(pointerKey(context, shard),
                AerospikeConstants.LOAD_POINTER, 1L);
        if (Objects.isNull(record)) {
            throw MagazineExceptions.magazineUnprepared(
                    String.format(ErrorMessage.ERROR_READING_POINTERS, context.getMagazineIdentifier()));
        }
        return record.getLong(AerospikeConstants.LOAD_POINTER);
    }

    public void incrementLoadCounter(final MagazineContext context, final Integer shard)
            throws ExecutionException, RetryException {
        adjustCounter(context, shard, AerospikeConstants.LOAD_COUNTER, 1L);
    }

    public void incrementFireCounter(final MagazineContext context, final Integer shard)
            throws ExecutionException, RetryException {
        adjustCounter(context, shard, AerospikeConstants.FIRE_COUNTER, 1L);
    }

    public void decrementFireCounter(final MagazineContext context, final Integer shard)
            throws ExecutionException, RetryException {
        adjustCounter(context, shard, AerospikeConstants.FIRE_COUNTER, -1L);
    }

    public Map<String, MetaData> readAll(final MagazineContext context)
            throws ExecutionException, RetryException {
        final Record[] pointerRecords = batchRead(context, AerospikeNaming.pointerSuffix(context));
        final Record[] counterRecords = AerospikeNaming.usesUnifiedMetadata(context)
                ? pointerRecords
                : batchRead(context, AerospikeConstants.COUNTERS);

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
     * {@code FIRE_COUNTER} can never over-count: the unified schema advances it atomically with the
     * pointer and only when a data record was found, and the legacy schema can only under-count on
     * crash, which fails open.
     *
     * @return shard numbers with data available to fire, ascending.
     */
    public int[] activeShards(final MagazineContext context) throws ExecutionException, RetryException {
        final Record[] pointerRecords = batchRead(context, AerospikeNaming.pointerSuffix(context));
        final Record[] counterRecords = AerospikeNaming.usesUnifiedMetadata(context)
                ? pointerRecords
                : batchRead(context, AerospikeConstants.COUNTERS);

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
            final String counterBin,
            final long delta) throws ExecutionException, RetryException {
        final Key key = new Key(namespace, metaSetName,
                AerospikeNaming.name(context, shard, AerospikeNaming.counterSuffix(context)));
        if (Objects.isNull(operateOnMetadata(key, counterBin, delta))) {
            throw MagazineExceptions.magazineUnprepared(
                    String.format(ErrorMessage.ERROR_READING_COUNTERS, context.getMagazineIdentifier()));
        }
    }

    private Record operateOnMetadata(final Key key, final String bin, final long delta)
            throws ExecutionException, RetryException {
        return retryerFactory.call(() -> {
            final WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            writePolicy.expiration = metaDataTtl;
            return client.operate(writePolicy, key,
                    Operation.add(new Bin(bin, delta)),
                    Operation.get(bin));
        });
    }

    private Record[] batchRead(final MagazineContext context, final String suffix)
            throws ExecutionException, RetryException {
        return retryerFactory.call(() -> client.get(client.getBatchPolicyDefault(),
                AerospikeNaming.metaKeys(namespace, metaSetName, context, suffix)));
    }

    private Key pointerKey(final MagazineContext context, final Integer shard) {
        return new Key(namespace, metaSetName,
                AerospikeNaming.name(context, shard, AerospikeNaming.pointerSuffix(context)));
    }

    private static long longOrZero(final Record record, final String bin) {
        return Objects.nonNull(record) ? record.getLong(bin) : 0L;
    }

    /** Initial capacity that avoids a rehash for the given element count at the default 0.75f. */
    private static int capacityFor(final int expectedSize) {
        return (int) Math.ceil(expectedSize / 0.75d) + 1;
    }
}
