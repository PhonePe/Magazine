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

package com.phonepe.magazine.entity;

import lombok.Getter;

/**
 * Descriptive label for a storage backend. Dispatch happens through
 * {@link com.phonepe.magazine.core.BaseMagazineStorage} polymorphism, not through this enum.
 */
@Getter
public enum StorageType {
    AEROSPIKE(StorageType.AEROSPIKE_TEXT);

    public static final String AEROSPIKE_TEXT = "AEROSPIKE";

    private final String value;

    StorageType(final String value) {
        this.value = value;
    }
}
