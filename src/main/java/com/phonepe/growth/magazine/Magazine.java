package com.phonepe.growth.magazine;

import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import lombok.Builder;
import lombok.Data;

import java.util.Optional;

@Data
public class Magazine<T> {
    private final String clientId;
    private final BaseMagazineStorage<T> baseMagazineStorage;

    @Builder
    public Magazine(String clientId, BaseMagazineStorage<T> baseMagazineStorage) {
        this.clientId = clientId;
        this.baseMagazineStorage = baseMagazineStorage;
    }

    public boolean prepare(String keyPrefix) {
        return baseMagazineStorage.prepare(keyPrefix);
    }

    public boolean load(String keyPrefix, T data) {
        return baseMagazineStorage.load(keyPrefix, data);
    }

    public boolean reload(String keyPrefix, T data) {
        return baseMagazineStorage.reload(keyPrefix, data);
    }

    public Optional<T> fire(String keyPrefix) {
        return baseMagazineStorage.fire(keyPrefix);
    }

    public MetaData getMetaData(String keyPrefix) {
        return baseMagazineStorage.getMetaData(keyPrefix);
    }
}
