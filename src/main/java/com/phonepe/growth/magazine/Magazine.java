package com.phonepe.growth.magazine;

import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import lombok.Builder;
import lombok.Data;

import java.util.Map;
import java.util.Optional;

@Data
public class Magazine<T> {
    private final String clientId;
    private final BaseMagazineStorage<T> baseMagazineStorage;
    private final String magazineIdentifier;

    @Builder
    public Magazine(String clientId, BaseMagazineStorage<T> baseMagazineStorage, String magazineIdentifier) {
        this.clientId = clientId;
        this.baseMagazineStorage = baseMagazineStorage;
        this.magazineIdentifier = magazineIdentifier;
    }

    public boolean load(T data) {
        return baseMagazineStorage.load(magazineIdentifier, data);
    }

    public boolean reload(T data) {
        return baseMagazineStorage.reload(magazineIdentifier, data);
    }

    public Optional<T> fire() {
        return baseMagazineStorage.fire(magazineIdentifier);
    }

    public Map<String, MetaData> getMetaData() {
        return baseMagazineStorage.getMetaData(magazineIdentifier);
    }
}
