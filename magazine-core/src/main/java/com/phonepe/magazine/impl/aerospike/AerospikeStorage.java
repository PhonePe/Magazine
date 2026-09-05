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

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.github.rholder.retry.RetryException;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.entity.StorageType;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.exception.MagazineExceptions;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikeNaming;
import com.phonepe.magazine.impl.aerospike.common.AerospikeRetryerFactory;
import com.phonepe.magazine.impl.aerospike.common.ErrorMessage;
import com.phonepe.magazine.impl.aerospike.store.ActiveShardSelector;
import com.phonepe.magazine.impl.aerospike.store.DeDupeGuard;
import com.phonepe.magazine.impl.aerospike.store.MagazineDataStore;
import com.phonepe.magazine.impl.aerospike.store.MagazineMetadataStore;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Aerospike-backed magazine storage.
 * <p>
 * This class orchestrates only. The pieces it coordinates are:
 * <ul>
 *   <li>{@link com.phonepe.magazine.impl.aerospike.common.AerospikeNaming} - key and set naming</li>
 *   <li>{@link MagazineMetadataStore} - pointers, counters and the fire-pointer claim</li>
 *   <li>{@link MagazineDataStore} - the payload records</li>
 *   <li>{@link ActiveShardSelector} - which shard to fire from</li>
 *   <li>{@link DeDupeGuard} - duplicate suppression on load</li>
 *   <li>{@link AerospikeMagazineInitializer} - per-magazine configuration resolution</li>
 * </ul>
 * A single instance may serve many magazines, so it holds no per-magazine state; everything
 * magazine-specific arrives in the {@link MagazineContext}.
 */
@Slf4j
@EqualsAndHashCode(callSuper = true, onlyExplicitlyIncluded = true)
public class AerospikeStorage<T> extends BaseMagazineStorage<T> {

    @Getter
    private final String namespace;
    @Getter
    private final String dataSetName;
    @Getter
    private final String metaSetName;

    private final Class<T> clazz;
    private final int maxFireContentionAttempts;
    private final int maxFireHoleSkips;
    private final AerospikeMagazineInitializer initializer;
    private final MagazineMetadataStore metadataStore;
    private final MagazineDataStore<T> dataStore;
    private final ActiveShardSelector activeShards;
    private final DeDupeGuard<T> deDupeGuard;

    @Builder
    public AerospikeStorage(
            final IAerospikeClient aerospikeClient,
            final AerospikeStorageConfig storageConfig,
            final boolean enableDeDupe,
            final String farmId,
            final Class<T> clazz,
            final String clientId,
            final MagazineScope scope) {
        super(StorageType.AEROSPIKE, AerospikeStorageValidator.validateConfig(storageConfig).getRecordTtl(),
                storageConfig.getMetaDataTtl(), farmId, enableDeDupe, clientId, scope);
        AerospikeStorageValidator.validateStorage(aerospikeClient, clazz, enableDeDupe);

        this.clazz = clazz;
        this.maxFireContentionAttempts = storageConfig.getMaxFireContentionAttempts();
        this.maxFireHoleSkips = storageConfig.getMaxFireHoleSkips();
        this.namespace = storageConfig.getNamespace();
        this.dataSetName = AerospikeNaming.resolveSetName(storageConfig.getDataSetName(), farmId, scope);
        this.metaSetName = AerospikeNaming.resolveSetName(storageConfig.getMetaSetName(), farmId, scope);

        final AerospikeRetryerFactory retryerFactory = new AerospikeRetryerFactory();
        this.initializer = new AerospikeMagazineInitializer(aerospikeClient, retryerFactory,
                namespace, metaSetName, storageConfig.getShards(), storageConfig.isAllowShardIncrease());
        this.metadataStore = new MagazineMetadataStore(aerospikeClient, retryerFactory,
                namespace, metaSetName, getMetaDataTtl());
        this.dataStore = new MagazineDataStore<>(aerospikeClient, retryerFactory,
                namespace, dataSetName, getRecordTtl(), clazz);
        this.activeShards = new ActiveShardSelector(this::loadActiveShards);

        this.deDupeGuard = enableDeDupe
                ? DeDupeGuard.aerospike(aerospikeClient, retryerFactory, namespace,
                        farmId, clientId, scope, getRecordTtl())
                : DeDupeGuard.disabled();
    }

    @Override
    public MagazineContext initialize(final String magazineIdentifier) {
        return initializer.initialize(magazineIdentifier);
    }

    @Override
    public boolean load(final MagazineContext context, final T data) {
        validateDataType(data);
        try (DeDupeGuard.Handle handle = deDupeGuard.acquire(context.getMagazineIdentifier(), data)) {
            if (handle.alreadyLoaded()) {
                return true;
            }
            final boolean loaded = append(context, data);
            if (loaded) {
                handle.remember();
            }
            return loaded;
        } catch (Exception e) {
            throw mapFailure(e, ErrorMessage.ERROR_LOADING_DATA, context);
        }
    }

    @Override
    public boolean reload(final MagazineContext context, final T data) {
        validateDataType(data);
        try (DeDupeGuard.Handle handle = deDupeGuard.acquire(context.getMagazineIdentifier(), data)) {
            // Reload republishes an already-counted payload, so the load counter must not move.
            // Decrementing the fire counter restores the pending balance instead.
            final Integer shard = selectShard(context);
            final long loadPointer = metadataStore.incrementAndGetLoadPointer(context, shard);
            final boolean loaded = dataStore.write(context, shard, loadPointer, data);
            if (loaded) {
                metadataStore.decrementFireCounter(context, shard);
            }
            return loaded;
        } catch (Exception e) {
            throw mapFailure(e, ErrorMessage.ERROR_LOADING_DATA, context);
        }
    }

    /**
     * Claims the next record on a randomly chosen active shard.
     * <p>
     * Two retry budgets are tracked because they bound different failure modes:
     * <ul>
     *   <li><b>contention</b> - the pointer claim was lost, nothing advanced. Backed off with full
     *       jitter and bounded, since unbounded retries here are a thundering herd.</li>
     *   <li><b>hole skips</b> - the claim was won but the data record was absent, so the pointer
     *       advanced past a slot whose write had failed. That <em>is</em> forward progress, so it
     *       must not consume the contention budget or a sparse shard would fail spuriously.</li>
     * </ul>
     * Exhausting either yields {@code RETRIES_EXHAUSTED} - "gave up, data may still exist" -
     * deliberately distinct from {@code NOTHING_TO_FIRE}, raised only when every shard is drained.
     */
    @Override
    public MagazineData<T> fire(final MagazineContext context) {
        int contentionAttempts = 0;
        int holeSkips = 0;
        try {
            while (true) {
                final Integer shard = activeShards.randomShardForFire(context);
                final Record metadata = metadataStore.readPointers(context, shard);
                if (Objects.isNull(metadata)) {
                    throw MagazineMetadataStore.missingMetadata(context, shard);
                }

                final long firePointer = metadata.getLong(AerospikeConstants.FIRE_POINTER);
                if (firePointer >= metadata.getLong(AerospikeConstants.LOAD_POINTER)) {
                    activeShards.suppress(context, shard);
                    continue;
                }

                final long claimedPointer = firePointer + 1;
                final Record dataRecord = dataStore.read(context, shard, claimedPointer);
                if (!metadataStore.claimFirePointer(context, shard, firePointer, Objects.nonNull(dataRecord))) {
                    if (++contentionAttempts >= maxFireContentionAttempts) {
                        throw MagazineExceptions.retriesExhausted(
                                String.format(ErrorMessage.ERROR_FIRING_DATA, context.getMagazineIdentifier()));
                    }
                    backoff(contentionAttempts);
                    continue;
                }

                contentionAttempts = 0; // progress resets the contention budget
                if (Objects.isNull(dataRecord)) {
                    if (++holeSkips >= maxFireHoleSkips) {
                        throw MagazineExceptions.retriesExhausted(
                                String.format(ErrorMessage.ERROR_FIRING_DATA, context.getMagazineIdentifier()));
                    }
                    continue; // advanced past a hole - no backoff, this was forward progress
                }

                if (!AerospikeNaming.usesUnifiedMetadata(context)) {
                    // Legacy split schema cannot advance pointer and counter atomically; a crash
                    // here under-counts fires, which only inflates reported pending.
                    metadataStore.incrementFireCounter(context, shard);
                }
                return MagazineData.<T>builder()
                        .firePointer(claimedPointer)
                        .shard(shard)
                        .magazineIdentifier(context.getMagazineIdentifier())
                        .data(clazz.cast(dataRecord.getValue(AerospikeConstants.DATA)))
                        .build();
            }
        } catch (Exception e) {
            throw mapFailure(e, ErrorMessage.ERROR_FIRING_DATA, context);
        }
    }

    @Override
    public Map<String, MetaData> getMetaData(final MagazineContext context) {
        try {
            return metadataStore.readAll(context);
        } catch (Exception e) {
            throw mapFailure(e, ErrorMessage.ERROR_GETTING_META_DATA, context);
        }
    }

    @Override
    public void delete(final MagazineContext context, final MagazineData<T> magazineData) {
        if (Objects.isNull(magazineData)) {
            throw MagazineExceptions.invalidConfiguration("Magazine data is required.");
        }
        if (!context.getMagazineIdentifier().equals(magazineData.getMagazineIdentifier())) {
            throw MagazineExceptions.invalidConfiguration("Magazine data belongs to a different magazine.");
        }
        try {
            dataStore.delete(context, magazineData);
        } catch (Exception e) {
            throw mapFailure(e, ErrorMessage.ERROR_DELETING_DATA, context);
        }
    }

    @Override
    public Set<MagazineData<T>> peek(final MagazineContext context,
            final Map<Integer, Set<Long>> shardPointersMap) {
        try {
            return dataStore.peek(context, shardPointersMap);
        } catch (Exception e) {
            throw mapFailure(e, ErrorMessage.ERROR_PEEKING_DATA, context);
        }
    }

    private boolean append(final MagazineContext context, final T data)
            throws ExecutionException, RetryException {
        final Integer shard = selectShard(context);
        final long loadPointer = metadataStore.incrementAndGetLoadPointer(context, shard);
        final boolean written = dataStore.write(context, shard, loadPointer, data);
        if (written) {
            metadataStore.incrementLoadCounter(context, shard);
        }
        return written;
    }

    private int[] loadActiveShards(final MagazineContext context) {
        try {
            return metadataStore.activeShards(context);
        } catch (Exception e) {
            throw mapFailure(e, ErrorMessage.ERROR_GETTING_META_DATA, context);
        }
    }

    /** null for an unsharded magazine, so keys omit the shard fragment entirely. */
    private static Integer selectShard(final MagazineContext context) {
        return context.isUnsharded()
                ? null
                : ThreadLocalRandom.current().nextInt(context.getShards());
    }

    /**
     * Capped exponential backoff with full jitter. Full jitter rather than additive is what
     * actually decorrelates competing firers - without it they retry in lockstep.
     */
    private static void backoff(final int attempt) throws InterruptedException {
        final long ceiling = Math.min(AerospikeConstants.FIRE_BACKOFF_MAX_MS,
                AerospikeConstants.FIRE_BACKOFF_BASE_MS * (1L << Math.min(attempt, 16)));
        TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(1, ceiling + 1));
    }

    private void validateDataType(final T data) {
        if (!clazz.isInstance(data)) {
            throw MagazineExceptions.dataTypeMismatch("Mismatch in data type of magazine and requested data.");
        }
    }

    private MagazineException mapFailure(final Exception exception,
            final String errorMessage,
            final MagazineContext context) {
        final String message = String.format(errorMessage, context.getMagazineIdentifier());
        if (exception instanceof InterruptedException || isInterrupted(exception)) {
            Thread.currentThread().interrupt();
            return MagazineExceptions.retriesExhausted(message, exception);
        }
        if (exception instanceof MagazineException || exception.getCause() instanceof MagazineException) {
            return MagazineException.propagate(exception);
        }
        if (exception instanceof RetryException) {
            return MagazineExceptions.retriesExhausted(message, exception);
        }
        if (exception instanceof ExecutionException) {
            return MagazineExceptions.connectionError(message, exception);
        }
        return MagazineException.propagate(exception);
    }

    private static boolean isInterrupted(final Throwable throwable) {
        for (Throwable cause = throwable; Objects.nonNull(cause); cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }
}
