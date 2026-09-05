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

import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.WritePolicy;
import com.github.rholder.retry.RetryException;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.exception.MagazineExceptions;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikeNaming;
import com.phonepe.magazine.impl.aerospike.common.AerospikeRetryerFactory;
import com.phonepe.magazine.impl.aerospike.common.ErrorMessage;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;

/**
 * Reads and writes the payload records themselves, keyed by {@code <magazine>[_SHARD_<n>]_<pointer>}.
 */
@RequiredArgsConstructor(access = lombok.AccessLevel.PUBLIC)
public final class MagazineDataStore<T> {

    private final IAerospikeClient client;
    private final AerospikeRetryerFactory retryerFactory;
    private final String namespace;
    private final String dataSetName;
    private final int recordTtl;
    private final Class<T> clazz;

    public boolean write(final MagazineContext context,
            final Integer shard,
            final long pointer,
            final T data) throws ExecutionException, RetryException {
        final Key key = dataKey(context, shard, pointer);
        return retryerFactory.call(() -> {
            final WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
            writePolicy.expiration = recordTtl;
            writePolicy.sendKey = true;
            client.put(writePolicy, key,
                    new Bin(AerospikeConstants.DATA, data),
                    new Bin(AerospikeConstants.MODIFIED_AT, System.currentTimeMillis()));
            return true;
        });
    }

    /** @return the record at that pointer, or null when the slot is a hole. */
    public Record read(final MagazineContext context, final Integer shard, final long pointer)
            throws ExecutionException, RetryException {
        final Key key = dataKey(context, shard, pointer);
        return retryerFactory.call(() -> client.get(client.getReadPolicyDefault(), key));
    }

    public void delete(final MagazineContext context, final MagazineData<T> magazineData)
            throws ExecutionException, RetryException {
        final Key key = dataKey(context, magazineData.getShard(), magazineData.getFirePointer());
        retryerFactory.call(() -> {
            client.delete(new WritePolicy(client.getWritePolicyDefault()), key);
            return true;
        });
    }

    public Set<MagazineData<T>> peek(final MagazineContext context,
            final Map<Integer, Set<Long>> shardPointersMap) throws ExecutionException, RetryException {
        final List<PeekRequest> requests = new ArrayList<>();
        final List<BatchRead> batchReads = new ArrayList<>();
        for (Map.Entry<Integer, Set<Long>> entry : shardPointersMap.entrySet()) {
            for (long pointer : entry.getValue()) {
                batchReads.add(new BatchRead(dataKey(context, entry.getKey(), pointer), true));
                requests.add(new PeekRequest(entry.getKey(), pointer));
            }
        }
        if (batchReads.isEmpty()) {
            return Set.of();
        }

        retryerFactory.call(() -> client.get(client.getBatchPolicyDefault(), batchReads));

        final Set<MagazineData<T>> peeked = new HashSet<>(capacityFor(requests.size()));
        for (int i = 0; i < requests.size(); i++) {
            final BatchRead batchRead = batchReads.get(i);
            if (batchRead.resultCode != ResultCode.OK
                    && batchRead.resultCode != ResultCode.KEY_NOT_FOUND_ERROR) {
                throw MagazineExceptions.connectionError(
                        String.format(ErrorMessage.ERROR_PEEKING_DATA, context.getMagazineIdentifier()), null);
            }
            final Record record = batchRead.record;
            if (Objects.nonNull(record)) {
                final PeekRequest request = requests.get(i);
                peeked.add(new MagazineData<>(
                        clazz.cast(record.getValue(AerospikeConstants.DATA)),
                        request.pointer(),
                        request.shard(),
                        context.getMagazineIdentifier()));
            }
        }
        return peeked;
    }

    private Key dataKey(final MagazineContext context, final Integer shard, final long pointer) {
        return new Key(namespace, dataSetName,
                AerospikeNaming.name(context, shard, String.valueOf(pointer)));
    }

    /** Initial capacity that avoids a rehash for the given element count at the default 0.75f. */
    private static int capacityFor(final int expectedSize) {
        return (int) Math.ceil(expectedSize / 0.75d) + 1;
    }

    private record PeekRequest(Integer shard, long pointer) {
    }
}
