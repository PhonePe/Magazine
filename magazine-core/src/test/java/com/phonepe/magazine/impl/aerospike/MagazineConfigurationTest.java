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

import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Configuration and payload-type validation performed at construction time.
 */
class MagazineConfigurationTest extends AerospikeMagazineTestBase {

    @Test
    void configurationValidationTest() {
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () ->
                Magazine.<String>builder()
                        .magazineIdentifier("MAGAZINE_ID")
                        .build());
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () ->
                Magazine.<String>builder()
                        .magazineIdentifier(" ")
                        .baseMagazineStorage(buildMagazineStorage(String.class, false))
                        .build());
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(null, String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig(null, "DATA_SET", "META_SET", 100, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", " ", "META_SET", 100, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", " ", 100, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 0, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 200, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_SHARDS, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 0), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), String.class,
                false, " ", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), String.class,
                false, "FARM_ID", null, MagazineScope.LOCAL, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", null, aerospikeClient));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), String.class,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, null));
        assertMagazineError(ErrorCode.INVALID_CONFIGURATION, () -> buildStorage(
                buildStorageConfig("NAMESPACE", "DATA_SET", "META_SET", 100, 200, 1), null,
                false, "FARM_ID", "CLIENT_ID", MagazineScope.LOCAL, aerospikeClient));
    }

    @Test
    void notImplementedGlobalScopeTest() {
        // The lambda wraps only the AerospikeStorage build: that is what validates scope and
        // throws. Magazine.builder() never runs - a lambda that lexically contains two throwing
        // calls (this one, and Magazine's own .build()) would leave it ambiguous which is under
        // test, even though only one of them actually executes before the throw.
        MagazineException exception = assertThrows(MagazineException.class, () -> AerospikeStorage.<Long>builder()
                .clazz(Long.class)
                .storageConfig(AerospikeStorageConfig.builder()
                        .dataSetName("DATA_SET")
                        .metaSetName("META_SET")
                        .namespace("NAMESPACE")
                        .shards(16)
                        .build())
                .aerospikeClient(aerospikeClient)
                .enableDeDupe(true)
                .farmId("FARM_ID")
                .clientId("CLIENT_ID")
                .scope(MagazineScope.GLOBAL)
                .build());
        assertEquals(ErrorCode.NOT_IMPLEMENTED, exception.getErrorCode());
    }
}
