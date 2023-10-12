package com.phonepe.growth.magazine.impl.hbase;

import com.phonepe.growth.magazine.common.MagazineData;
import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.StorageType;
import com.phonepe.growth.magazine.scope.MagazineScope;

import java.util.Map;
import java.util.Set;

public class HBaseStorage<T> extends BaseMagazineStorage<T> {

    public HBaseStorage(final int recordTtl,
            final int metaDataTtl,
            final String farmId,
            final boolean deDupeEnabled,
            final int shards,
            final String clientId,
            final
            MagazineScope scope) {
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
