package com.phonepe.growth.magazine.impl.hbase;

import com.phonepe.growth.magazine.core.BaseMagazine;
import com.phonepe.growth.magazine.core.MagazineType;

import java.util.Optional;

public class HBaseMagazine extends BaseMagazine {

    public HBaseMagazine() {
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
