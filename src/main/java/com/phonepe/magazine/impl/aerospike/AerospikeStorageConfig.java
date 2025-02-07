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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AerospikeStorageConfig {

    @NotBlank
    private String namespace;
    @NotBlank
    private String metaSetName;
    @NotBlank
    private String dataSetName;
    @Min(-2)
    @Builder.Default
    private int recordTtl = 30 * 24 * 60 * 60;  // 30 days default ttl
    @Min(1)
    @Builder.Default
    private int shards = 64; //Default 64 shards in a magazine
    @Min(-2)
    @Builder.Default
    private int metaDataTtl = 2 * 30 * 24 * 60 * 60; // 2 months default TTL
}
