package com.phonepe.growth.magazine;

import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import lombok.Builder;
import lombok.Data;

import java.util.Optional;

@Data
public class Magazine {
    private final String clientId;
    private final BaseMagazineStorage baseMagazineStorage;

    @Builder
    public Magazine(String clientId, BaseMagazineStorage baseMagazineStorage) {
        this.clientId = clientId;
        this.baseMagazineStorage = baseMagazineStorage;
    }

    public boolean prepare(String keyPrefix) {
        return baseMagazineStorage.prepare(keyPrefix);
    }

    public boolean load(String keyPrefix, Object data) {
        return baseMagazineStorage.load(keyPrefix, data);
    }

    public boolean reload(String keyPrefix, Object data) {
        return baseMagazineStorage.reload(keyPrefix, data);
    }

    public Optional<Object> fire(String keyPrefix) {
        return baseMagazineStorage.fire(keyPrefix);
    }

    public MetaData getMetaData(String keyPrefix) {
        return baseMagazineStorage.getMetaData(keyPrefix);
    }
}
