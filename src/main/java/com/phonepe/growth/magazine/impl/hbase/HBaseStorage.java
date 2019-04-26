package com.phonepe.growth.magazine.impl.hbase;

import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.MagazineType;

import java.util.Optional;

public class HBaseStorage extends BaseMagazineStorage {

    public HBaseStorage() {
        super(MagazineType.HBASE);
    }

    @Override
    public boolean prepare(String keyPrefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean load(String keyPrefix, Object data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Object> fire(String keyPrefix) {
        throw new UnsupportedOperationException();
    }
}
