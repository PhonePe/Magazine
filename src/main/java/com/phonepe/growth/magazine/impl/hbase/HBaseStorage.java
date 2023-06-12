package com.phonepe.growth.magazine.impl.hbase;

import com.phonepe.growth.magazine.common.MagazineData;
import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.StorageType;

import java.util.Map;

public class HBaseStorage<T> extends BaseMagazineStorage<T> {

    public HBaseStorage(final int recordTtl, final int metaDataTtl, final String farmId,
                        final boolean deDupeEnabled, final int shards, final String clientId) {
        super(StorageType.HBASE, recordTtl, metaDataTtl, farmId, deDupeEnabled, shards, clientId);
    }

    @Override
    public boolean load(final String keyPrefix, final T data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean reload(final String keyPrefix, final T data) {
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
}
