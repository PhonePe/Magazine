package com.phonepe.growth.magazine.impl.hbase;

import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.StorageType;

import java.util.Optional;

public class HBaseStorage<T> extends BaseMagazineStorage<T> {

    public HBaseStorage(int recordTtl, boolean deDupeEnabled) {
        super(StorageType.HBASE, recordTtl, deDupeEnabled);
    }

    @Override
    public boolean load(String keyPrefix, T data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean reload(String keyPrefix, T data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<T> fire(String keyPrefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MetaData getMetaData(String keyPrefix) {
        throw new UnsupportedOperationException();
    }
}
