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
import com.aerospike.client.Key;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.impl.aerospike.common.AerospikeConstants;
import com.phonepe.magazine.impl.aerospike.common.AerospikeNaming;
import com.phonepe.magazine.server.AerospikeTestContainer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.function.Executable;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Fixture and builders shared by the Aerospike magazine tests.
 * <p>
 * These used to live alongside 35 test methods in a single 1000-line class, which made it hard to
 * see which helper any given test actually depended on.
 */
abstract class AerospikeMagazineTestBase {

    /**
     * Shared across every test class in the JVM: one container, one client. Building a client per
     * test method leaked a connection pool and a tend thread apiece.
     */
    protected final IAerospikeClient aerospikeClient = AerospikeTestContainer.client();

    protected final Random random = ThreadLocalRandom.current();

    /**
     * Builds the data-record key through the production key layout, so a divergence would fail.
     */
    protected String dataKey(final MagazineData<String> data, final int shards) {
        return AerospikeNaming.name(
                new MagazineContext(data.getMagazineIdentifier(),
                        AerospikeConstants.UNIFIED_METADATA_SCHEMA_VERSION, shards),
                data.getShard(), String.valueOf(data.getFirePointer()));
    }

    protected <T> BaseMagazineStorage<T> buildMagazineStorage(Class<T> clazz) {
        return buildMagazineStorage(clazz, true);
    }

    protected <T> AerospikeStorage<T> buildMagazineStorage(Class<T> clazz, boolean enableDeDupe) {
        return buildStorage(buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET",
                        30 * 24 * 60 * 60, 2 * 30 * 24 * 60 * 60, 16),
                clazz, enableDeDupe, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient);
    }

    protected Magazine<String> buildUnshardedMagazine(final String magazineIdentifier,
                                                      final String dataSetName,
                                                      final String metaSetName) {
        return Magazine.<String>builder()
                .magazineIdentifier(magazineIdentifier)
                .baseMagazineStorage(buildStorage(
                        buildStorageConfig("NAMESPACE", dataSetName, metaSetName,
                                30 * 24 * 60 * 60, 2 * 30 * 24 * 60 * 60, 1),
                        String.class, false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient))
                .build();
    }

    protected <T> AerospikeStorage<T> buildStorage(final AerospikeStorageConfig config,
                                                   final Class<T> clazz,
                                                   final boolean enableDeDupe,
                                                   final String farmId,
                                                   final String clientId,
                                                   final MagazineScope scope,
                                                   final IAerospikeClient client) {
        return buildStorage(config, clazz, enableDeDupe, farmId, clientId, scope, client, null);
    }

    protected <T> AerospikeStorage<T> buildStorage(final AerospikeStorageConfig config,
                                                   final Class<T> clazz,
                                                   final boolean enableDeDupe,
                                                   final String farmId,
                                                   final String clientId,
                                                   final MagazineScope scope,
                                                   final IAerospikeClient client,
                                                   final MeterRegistry meterRegistry) {
        return AerospikeStorage.<T>builder()
                .clazz(clazz)
                .storageConfig(config)
                .aerospikeClient(client)
                .enableDeDupe(enableDeDupe)
                .farmId(farmId)
                .clientId(clientId)
                .scope(scope)
                .meterRegistry(meterRegistry)
                .build();
    }

    /**
     * @return the counter's value, or 0 when it was never registered.
     */
    protected static double counterValue(final SimpleMeterRegistry registry,
                                         final String name,
                                         final String tagKey,
                                         final String tagValue) {
        return registry.find(name).tag(tagKey, tagValue).counters().stream()
                .mapToDouble(Counter::count)
                .sum();
    }

    protected AerospikeStorageConfig buildStorageConfig(final String namespace,
                                                        final String dataSetName,
                                                        final String metaSetName,
                                                        final int recordTtl,
                                                        final int metaDataTtl,
                                                        final int shards) {
        return AerospikeStorageConfig.builder()
                .namespace(namespace)
                .dataSetName(dataSetName)
                .metaSetName(metaSetName)
                .recordTtl(recordTtl)
                .metaDataTtl(metaDataTtl)
                .shards(shards)
                .build();
    }

    protected void deleteOnlyLoadedRecord(final Magazine<String> magazine) {
        Map.Entry<String, MetaData> loadedShard = magazine.getMetaData().entrySet().stream()
                .filter(entry -> entry.getValue().getLoadPointer() > 0)
                .findFirst()
                .orElseThrow();
        int shard = Integer.parseInt(loadedShard.getKey().substring(loadedShard.getKey().indexOf('_') + 1));
        String key = "%s_SHARD_%d_%d".formatted(
                magazine.getMagazineIdentifier(), shard, loadedShard.getValue().getLoadPointer());
        assertTrue(aerospikeClient.delete(
                aerospikeClient.getWritePolicyDefault(),
                new Key("NAMESPACE", "FARM_ID_DATA_SET", key)));
    }

    protected void assertMagazineError(final ErrorCode errorCode, final Executable action) {
        MagazineException exception = assertThrows(MagazineException.class, action);
        assertEquals(errorCode, exception.getErrorCode());
    }

    protected MetaData collectMetaData(Map<String, MetaData> metaDataMap) {
        return MetaData.builder()
                .loadPointer(metaDataMap.values()
                        .stream()
                        .mapToLong(MetaData::getLoadPointer)
                        .sum())
                .loadCounter(metaDataMap.values()
                        .stream()
                        .mapToLong(MetaData::getLoadCounter)
                        .sum())
                .firePointer(metaDataMap.values()
                        .stream()
                        .mapToLong(MetaData::getFirePointer)
                        .sum())
                .fireCounter(metaDataMap.values()
                        .stream()
                        .mapToLong(MetaData::getFireCounter)
                        .sum())
                .build();
    }
}
