package com.phonepe.growth.magazine.impl.hbase;

import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.StorageType;

import java.util.Map;
import java.util.Optional;

public class HBaseStorage<T> extends BaseMagazineStorage<T> {

    public HBaseStorage(final int recordTtl, final boolean deDupeEnabled, final int shards) {
        super(StorageType.HBASE, recordTtl, deDupeEnabled, shards);
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
    public Optional<T> fire(final String keyPrefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, MetaData> getMetaData(final String keyPrefix) {
        throw new UnsupportedOperationException();
    }
}
