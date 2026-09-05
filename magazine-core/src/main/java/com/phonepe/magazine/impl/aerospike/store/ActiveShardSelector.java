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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.exception.MagazineExceptions;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.ErrorMessage;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Caches which shards currently hold data and picks one at random to fire from.
 * <p>
 * The cache is refreshed periodically rather than on every fire, so it is deliberately allowed to
 * be stale: {@link #suppress} prunes a shard the caller has just observed to be drained, which
 * converges without waiting for the next refresh. Once every shard is pruned the next call raises
 * {@code NOTHING_TO_FIRE}, which is how an exhausted magazine terminates the fire loop.
 */
public final class ActiveShardSelector {

    private final AsyncLoadingCache<MagazineContext, int[]> cache;

    public ActiveShardSelector(final Function<MagazineContext, int[]> loader) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(AerospikeConstants.DEFAULT_MAX_ELEMENTS)
                .refreshAfterWrite(AerospikeConstants.DEFAULT_REFRESH, TimeUnit.SECONDS)
                .buildAsync(loader::apply);
    }

    /**
     * @return a randomly chosen active shard, or null when the magazine is unsharded.
     * @throws com.phonepe.magazine.exception.MagazineException with {@code NOTHING_TO_FIRE} when
     *         no shard has data.
     */
    public Integer randomShardForFire(final MagazineContext context)
            throws InterruptedException, ExecutionException {
        final int[] activeShards = cache.get(context).get();
        if (activeShards.length == 0) {
            throw MagazineExceptions.nothingToFire(
                    String.format(ErrorMessage.NO_DATA_TO_FIRE, context.getMagazineIdentifier()));
        }
        return context.isUnsharded()
                ? null
                : activeShards[ThreadLocalRandom.current().nextInt(activeShards.length)];
    }

    /** Prune a shard the caller has just observed to be drained. No-op if not currently cached. */
    public void suppress(final MagazineContext context, final Integer shard) {
        final int target = Objects.isNull(shard) ? 0 : shard;
        cache.synchronous().asMap().computeIfPresent(context, (key, activeShards) -> {
            final int[] remaining = new int[activeShards.length];
            int kept = 0;
            for (int activeShard : activeShards) {
                if (activeShard != target) {
                    remaining[kept++] = activeShard;
                }
            }
            return kept == activeShards.length ? activeShards : Arrays.copyOf(remaining, kept);
        });
    }
}
