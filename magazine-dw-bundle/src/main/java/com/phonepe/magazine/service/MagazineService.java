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

package com.phonepe.magazine.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.MagazineManager;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.request.PeekRequest;
import com.phonepe.magazine.response.MagazineDescriptor;
import com.phonepe.magazine.response.MagazineMetadataResponse;
import com.phonepe.magazine.response.PeekResponse;
import com.phonepe.magazine.response.PeekedData;
import com.phonepe.magazine.response.ShardMetadata;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.NotFoundException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class MagazineService {

    private static final int MAX_PEEK_POINTERS = 1_000;

    private final MagazineManager magazineManager;
    private final ObjectMapper objectMapper;

    public MagazineService(final MagazineManager magazineManager, final ObjectMapper objectMapper) {
        this.magazineManager = magazineManager;
        this.objectMapper = objectMapper;
    }

    public List<MagazineDescriptor> descriptors() {
        return magazines().keySet().stream()
                .sorted()
                .map(MagazineDescriptor::new)
                .toList();
    }

    public MagazineMetadataResponse metadata(final String identifier) {
        final Map<String, ShardMetadata> shards = new LinkedHashMap<>();
        magazine(identifier).getMetaData().entrySet().stream()
                .sorted(Map.Entry.comparingByKey(MagazineService::compareShardIds))
                .forEach(entry -> shards.put(entry.getKey(), toShardMetadata(entry.getValue())));
        return new MagazineMetadataResponse(
                identifier,
                Collections.unmodifiableMap(shards),
                totalMetadata(shards.values()));
    }

    public PeekResponse peek(final String identifier, final PeekRequest request) {
        if (Objects.isNull(request) || Objects.isNull(request.pointers()) || request.pointers().isEmpty()) {
            throw new BadRequestException("pointers must contain at least one shard");
        }
        final Magazine<?> magazine = magazine(identifier);
        final int shards = magazine.getShards();

        int pointerCount = 0;
        final Map<Integer, Set<Long>> requestedPointers = new LinkedHashMap<>();
        for (Map.Entry<Integer, Set<Long>> entry : request.pointers().entrySet()) {
            final Integer shard = entry.getKey();
            final Set<Long> pointers = entry.getValue();
            if (Objects.isNull(shard) || shard < 0 || shard >= shards
                    || Objects.isNull(pointers) || pointers.isEmpty()
                    || pointers.stream().anyMatch(pointer -> Objects.isNull(pointer) || pointer < 1)) {
                throw new BadRequestException("shards must be in range and pointers must be positive");
            }
            pointerCount += pointers.size();
            if (pointerCount > MAX_PEEK_POINTERS) {
                throw new BadRequestException("peek exceeds maximum pointer count of " + MAX_PEEK_POINTERS);
            }
            requestedPointers.put(shard, pointers);
        }

        final List<PeekedData> data = magazine.peek(toStorageShards(requestedPointers, shards)).stream()
                .map(this::toPeekedData)
                .sorted(Comparator.comparing(PeekedData::shard, Comparator.nullsFirst(Integer::compareTo))
                        .thenComparingLong(PeekedData::pointer))
                .toList();
        return new PeekResponse(identifier, data);
    }

    private static Map<Integer, Set<Long>> toStorageShards(final Map<Integer, Set<Long>> requested,
            final int shards) {
        if (shards > 1) {
            return requested;
        }
        return Collections.singletonMap(null, requested.getOrDefault(0, Set.of()));
    }

    private Magazine<?> magazine(final String identifier) {
        final Magazine<?> magazine = magazines().get(identifier);
        if (Objects.isNull(magazine)) {
            throw new NotFoundException("Magazine not configured for bundle: " + identifier);
        }
        return magazine;
    }

    private Map<String, Magazine<?>> magazines() {
        return magazineManager.getMagazineMap();
    }

    private static ShardMetadata toShardMetadata(final MetaData metadata) {
        final long pending = Math.max(0L, metadata.getLoadCounter() - metadata.getFireCounter());
        return new ShardMetadata(metadata.getLoadCounter(), metadata.getFireCounter(),
                metadata.getLoadPointer(), metadata.getFirePointer(), pending);
    }

    /**
     * Only counters aggregate. Pointers are per-shard monotonic sequences, so their sum has no
     * meaning - reporting it invites operators to read it as a queue depth. They are reported as
     * zero in the totals and shown per-shard instead.
     */
    private static ShardMetadata totalMetadata(final Iterable<ShardMetadata> shards) {
        long loadCounter = 0;
        long fireCounter = 0;
        long pending = 0;
        for (ShardMetadata shard : shards) {
            loadCounter += shard.loadCounter();
            fireCounter += shard.fireCounter();
            pending += shard.pending();
        }
        return new ShardMetadata(loadCounter, fireCounter, 0L, 0L, pending);
    }

    private static int compareShardIds(final String left, final String right) {
        final int leftNumber = shardNumber(left);
        final int rightNumber = shardNumber(right);
        return leftNumber >= 0 && rightNumber >= 0
                ? Integer.compare(leftNumber, rightNumber)
                : left.compareTo(right);
    }

    private static int shardNumber(final String shardId) {
        final int separator = shardId.lastIndexOf('_');
        if (separator < 0 || separator == shardId.length() - 1) {
            return -1;
        }
        for (int i = separator + 1; i < shardId.length(); i++) {
            if (!Character.isDigit(shardId.charAt(i))) {
                return -1;
            }
        }
        try {
            return Integer.parseInt(shardId, separator + 1, shardId.length(), 10);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private PeekedData toPeekedData(final MagazineData<?> data) {
        final Object payload = data.getData();
        final String type = Objects.isNull(payload) ? null : payload.getClass().getSimpleName();
        Object rendered;
        try {
            rendered = objectMapper.convertValue(payload, Object.class);
        } catch (IllegalArgumentException e) {
            rendered = String.valueOf(payload);
        }
        return new PeekedData(data.getShard(), data.getFirePointer(), type, rendered);
    }
}
