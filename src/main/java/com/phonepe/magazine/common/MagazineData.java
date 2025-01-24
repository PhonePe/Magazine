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
package com.phonepe.magazine.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * @author shantanu.tiwari
 * Created on 12/03/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MagazineData<T> {
    private T data;
    private long firePointer;
    private Integer shard;
    private String magazineIdentifier;

    public String createAerospikeKey() {
        return Objects.nonNull(shard)
                ? String.format("%s_%s_%d_%d", magazineIdentifier, Constants.SHARD_PREFIX, shard, firePointer)
                : String.format("%s_%d", magazineIdentifier, firePointer);
    }
}