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

package com.phonepe.magazine;

import com.aerospike.client.IAerospikeClient;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.entity.StorageType;
import com.phonepe.magazine.impl.aerospike.AerospikeStorage;
import com.phonepe.magazine.impl.aerospike.AerospikeStorageConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

class CoreRuntimeCompatibilityTest {

    /**
     * Smoke test that magazine-core is constructible under the Dropwizard dependency graph - it
     * guards against transitive resolution differences between the two modules, not against
     * storage behaviour. Shard count is deliberately not asserted here: it is a per-magazine
     * property resolved by {@code initialize()} against live storage, not a property of the
     * storage instance.
     */
    @Test
    void constructsCoreStorageWithDropwizardDependencyGraph() {
        final AerospikeStorage<String> storage = AerospikeStorage.<String>builder()
                .aerospikeClient(mock(IAerospikeClient.class))
                .storageConfig(AerospikeStorageConfig.builder()
                        .namespace("test")
                        .dataSetName("data")
                        .metaSetName("metadata")
                        .shards(4)
                        .build())
                .enableDeDupe(false)
                .farmId("farm")
                .clazz(String.class)
                .clientId("dashboard-test")
                .scope(MagazineScope.LOCAL)
                .build();

        assertEquals(StorageType.AEROSPIKE, storage.getType());
        assertEquals("test", storage.getNamespace());
        assertEquals("farm_data", storage.getDataSetName());
        assertFalse(storage.isEnableDeDupe());
    }
}
