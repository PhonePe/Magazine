package com.phonepe.growth.magazine.impl.hbase;

import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.MagazineType;

import java.util.Optional;

public class HBaseStorage<T> extends BaseMagazineStorage<T> {

    public HBaseStorage() {
        super(MagazineType.HBASE);
    }

    @Override
    public boolean prepare(String keyPrefix) {
        throw new UnsupportedOperationException();
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
