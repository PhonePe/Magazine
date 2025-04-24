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

package com.phonepe.magazine.impl.hbase;

import com.phonepe.magazine.common.MagazineData;
import com.phonepe.magazine.common.MetaData;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.core.StorageType;
import com.phonepe.magazine.scope.MagazineScope;

import java.util.Map;
import java.util.Set;

public class HBaseStorage<T> extends BaseMagazineStorage<T> {

    public HBaseStorage(final int recordTtl,
            final int metaDataTtl,
            final String farmId,
            final boolean deDupeEnabled,
            final int shards,
            final String clientId,
            final MagazineScope scope) {
        super(StorageType.HBASE, recordTtl, metaDataTtl, farmId, deDupeEnabled, shards, clientId, scope);
    }

    @Override
    public boolean load(final String keyPrefix,
            final T data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean reload(final String keyPrefix,
            final T data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MagazineData<T> fire(final String keyPrefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, MetaData> getMetaData(final String keyPrefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(MagazineData<T> magazineData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<MagazineData<T>> peek(final String magazineIdentifier,
            final Map<Integer, Set<Long>> shardPointersMap) {
        throw new UnsupportedOperationException();
    }
}
