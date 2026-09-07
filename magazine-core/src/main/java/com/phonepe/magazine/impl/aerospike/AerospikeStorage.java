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
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.entity.StorageType;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.exception.MagazineExceptions;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikeNaming;
import com.phonepe.magazine.impl.aerospike.common.AerospikeRetryer;
import com.phonepe.magazine.impl.aerospike.common.ErrorMessage;
import com.phonepe.magazine.impl.aerospike.store.ActiveShardSelector;
import com.phonepe.magazine.impl.aerospike.store.DeDupeGuard;
import com.phonepe.magazine.impl.aerospike.store.MagazineDataStore;
import com.phonepe.magazine.impl.aerospike.store.MagazineMetadataStore;
import com.phonepe.magazine.metrics.MagazineMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Aerospike-backed magazine storage.
 * <p>
 * Orchestration only; the work lives in the collaborators it builds. A single instance may serve
 * many magazines, so it holds no per-magazine state - everything magazine-specific arrives in the
 * {@link MagazineContext}.
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
    private final int maxFireHoleSkips;
    private final MagazineMetrics metrics;
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
            final MagazineScope scope,
            final MeterRegistry meterRegistry) {
        super(StorageType.AEROSPIKE, AerospikeStorageValidator.validateConfig(storageConfig).getRecordTtl(),
                storageConfig.getMetaDataTtl(), farmId, enableDeDupe, clientId, scope);
        AerospikeStorageValidator.validateStorage(aerospikeClient, clazz, enableDeDupe);

        this.clazz = clazz;
        this.maxFireHoleSkips = storageConfig.getMaxFireHoleSkips();
        this.namespace = storageConfig.getNamespace();
        this.dataSetName = AerospikeNaming.resolveSetName(storageConfig.getDataSetName(), farmId, scope);
        this.metaSetName = AerospikeNaming.resolveSetName(storageConfig.getMetaSetName(), farmId, scope);
        this.metrics = resolveMetrics(storageConfig.isMetricsEnabled(), meterRegistry);

        final AerospikeRetryer retryerFactory = new AerospikeRetryer();
        this.initializer = new AerospikeMagazineInitializer(aerospikeClient, retryerFactory,
                namespace, metaSetName, storageConfig.getShards(), storageConfig.isAllowShardIncrease());
        this.metadataStore = new MagazineMetadataStore(aerospikeClient, retryerFactory, metrics,
                namespace, metaSetName, getMetaDataTtl());
        this.dataStore = new MagazineDataStore<>(aerospikeClient, retryerFactory, metrics,
                namespace, dataSetName, getRecordTtl(), clazz);
        this.activeShards = new ActiveShardSelector(this::loadActiveShards,
                storageConfig.getActiveShardRefreshSeconds());

        this.deDupeGuard = enableDeDupe
                ? DeDupeGuard.aerospike(aerospikeClient, metrics, namespace,
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
        final long startNanos = metrics.startNanos();
        try (DeDupeGuard.Handle handle = deDupeGuard.claim(context.getMagazineIdentifier(), data)) {
            if (handle.duplicate()) {
                metrics.loadOutcome(context.getMagazineIdentifier(), MagazineMetrics.LoadOutcome.DUPLICATE);
                return true;
            }
            final boolean loaded = append(context, data);
            if (loaded) {
                // Confirming keeps the marker; without it the handle withdraws it on close so a
                // failed write does not suppress a legitimate retry until the TTL elapses.
                handle.confirm();
            }
            metrics.loadOutcome(context.getMagazineIdentifier(),
                    loaded ? MagazineMetrics.LoadOutcome.LOADED : MagazineMetrics.LoadOutcome.FAILED);
            return loaded;
        } catch (Exception e) {
            metrics.loadOutcome(context.getMagazineIdentifier(), MagazineMetrics.LoadOutcome.FAILED);
            throw mapFailure(e, ErrorMessage.ERROR_LOADING_DATA, context);
        } finally {
            metrics.recordLoad(startNanos, context.getMagazineIdentifier());
        }
    }

    /**
     * Republishes an already-counted payload.
     * <p>
     * Deliberately not deduplicated: reload exists precisely to load a payload that has been seen
     * before, so consulting the duplicate marker would suppress every call. Earlier releases took
     * and released a distributed lock here without ever reading it - two round trips that bought
     * nothing.
     */
    @Override
    public boolean reload(final MagazineContext context, final T data) {
        validateDataType(data);
        try {
            // The load counter must not move; decrementing the fire counter restores the pending
            // balance instead.
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
     * The claim is a single guarded atomic increment, so competing consumers receive distinct
     * pointers and a claim is never lost - there is no contention budget, no backoff, and the
     * round trips per dequeue do not scale with consumer count.
     * <p>
     * The one bounded loop left is <b>hole skips</b>: the claim succeeded but the data record was
     * absent because that slot's write had failed. Exhausting the budget yields
     * {@code RETRIES_EXHAUSTED} - "gave up, data may still exist" - deliberately distinct from
     * {@code NOTHING_TO_FIRE}, raised only when every shard is drained.
     */
    @Override
    public MagazineData<T> fire(final MagazineContext context) {
        final String magazineIdentifier = context.getMagazineIdentifier();
        final long startNanos = metrics.startNanos();
        int holeSkips = 0;
        try {
            while (true) {
                // The loop no longer blocks on anything that would throw InterruptedException, so
                // the interrupt has to be observed explicitly. Without this a consumer shutting
                // down mid-drain would keep skipping holes until its budget ran out.
                if (Thread.currentThread().isInterrupted()) {
                    throw MagazineExceptions.retriesExhausted(
                            String.format(ErrorMessage.ERROR_FIRING_DATA, magazineIdentifier));
                }
                final Integer shard = activeShards.randomShardForFire(context);
                final OptionalLong claimed = metadataStore.claimFirePointer(context, shard);
                if (claimed.isEmpty()) {
                    // Nothing left on this shard. Prune it rather than wait for the next refresh;
                    // once every shard is pruned the selector raises NOTHING_TO_FIRE.
                    metrics.fireClaim(magazineIdentifier, MagazineMetrics.ClaimOutcome.DRAINED);
                    activeShards.suppress(context, shard);
                    continue;
                }
                metrics.fireClaim(magazineIdentifier, MagazineMetrics.ClaimOutcome.WON);

                final long claimedPointer = claimed.getAsLong();
                final Record dataRecord = dataStore.read(context, shard, claimedPointer);
                if (Objects.isNull(dataRecord)) {
                    metrics.fireHoleSkip(magazineIdentifier);
                    if (++holeSkips >= maxFireHoleSkips) {
                        throw MagazineExceptions.retriesExhausted(
                                String.format(ErrorMessage.ERROR_FIRING_DATA, magazineIdentifier));
                    }
                    continue; // advanced past a hole - this was forward progress
                }

                // Cannot ride along with the claim: at claim time we do not yet know whether the
                // slot holds data, and only real deliveries may be counted. See
                // MagazineMetadataStore#incrementFireCounter for why under-counting is safe.
                metadataStore.incrementFireCounter(context, shard);
                metrics.fireOutcome(magazineIdentifier, MagazineMetrics.FireOutcome.DELIVERED);
                return MagazineData.<T>builder()
                        .firePointer(claimedPointer)
                        .shard(shard)
                        .magazineIdentifier(magazineIdentifier)
                        .data(clazz.cast(dataRecord.getValue(AerospikeConstants.DATA)))
                        .build();
            }
        } catch (Exception e) {
            final MagazineException failure = mapFailure(e, ErrorMessage.ERROR_FIRING_DATA, context);
            metrics.fireOutcome(magazineIdentifier, outcomeOf(failure));
            throw failure;
        } finally {
            metrics.recordFire(startNanos, magazineIdentifier);
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

    private boolean append(final MagazineContext context, final T data) {
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
     * Resolves where meters go.
     * <p>
     * A builder that was given no registry falls back to Micrometer's
     * {@link Metrics#globalRegistry}, so an application wires its backend once - the Dropwizard
     * bundle does this for you - instead of threading a registry through every storage it builds.
     * The global registry is a composite: with nothing attached it discards, and attaching a child
     * later still picks up meters registered before the attach, so bootstrap order does not matter.
     * <p>
     * {@code metricsEnabled=false} yields a private empty composite instead of a flag tested on
     * every record: its counters are no-ops, and because it is not the global registry it cannot
     * pick up a backend attached later.
     */
    private static MagazineMetrics resolveMetrics(final boolean metricsEnabled,
            final MeterRegistry meterRegistry) {
        if (!metricsEnabled) {
            return new MagazineMetrics(new CompositeMeterRegistry());
        }
        if (Objects.isNull(meterRegistry)) {
            log.debug("No MeterRegistry supplied; Magazine metrics will go to Micrometer's global "
                    + "registry. Attach a backend with Metrics.addRegistry(..), or set "
                    + "metricsEnabled false to opt out explicitly.");
            return new MagazineMetrics(Metrics.globalRegistry);
        }
        return new MagazineMetrics(meterRegistry);
    }

    /** Distinguishes a drained magazine from a genuine give-up in the outcome metric. */
    private static MagazineMetrics.FireOutcome outcomeOf(final MagazineException failure) {
        if (failure.getErrorCode() == ErrorCode.NOTHING_TO_FIRE) {
            return MagazineMetrics.FireOutcome.EMPTY;
        }
        return failure.getErrorCode() == ErrorCode.RETRIES_EXHAUSTED
                ? MagazineMetrics.FireOutcome.EXHAUSTED
                : MagazineMetrics.FireOutcome.FAILED;
    }

    private void validateDataType(final T data) {
        if (!clazz.isInstance(data)) {
            throw MagazineExceptions.dataTypeMismatch("Mismatch in data type of magazine and requested data.");
        }
    }

    /**
     * Normalises anything thrown on a storage path into a {@link MagazineException}.
     * <p>
     * An interrupt is reported as {@code RETRIES_EXHAUSTED} with the flag restored, so a consumer
     * shutting down stops rather than looping.
     */
    private MagazineException mapFailure(final Exception exception,
            final String errorMessage,
            final MagazineContext context) {
        if (exception instanceof MagazineException magazineException) {
            return magazineException;
        }
        final String message = String.format(errorMessage, context.getMagazineIdentifier());
        if (exception.getCause() instanceof MagazineException) {
            return MagazineException.propagate(exception);
        }
        if (isInterrupted(exception)) {
            Thread.currentThread().interrupt();
            return MagazineExceptions.retriesExhausted(message, exception);
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
