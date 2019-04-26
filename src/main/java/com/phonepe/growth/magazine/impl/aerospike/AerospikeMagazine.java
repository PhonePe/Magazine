package com.phonepe.growth.magazine.impl.aerospike;

import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.MagazineType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Optional;

@Data
@EqualsAndHashCode(callSuper = true)
public class AerospikeMagazine extends BaseMagazineStorage {

    private final AerospikeStore store;

    public AerospikeMagazine(AerospikeStore store) {
        super(MagazineType.AEROSPIKE);
        this.store = store;
    }

    @Override
    public boolean prepare(String keyPrefix) {
        return store.initPointers(keyPrefix);
    }

    @Override
    public boolean load(String keyPrefix, Object data) {
        return store.loadDataToAerospike(keyPrefix, data);
    }

    @Override
    public Optional<Object> fire(String keyPrefix) {
        return store.fireDataFromAerospike(keyPrefix);
    }
}
