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
    private final String magazineIdentifier;

    @Builder
    public Magazine(final String clientId, final BaseMagazineStorage<T> baseMagazineStorage, final String magazineIdentifier) {
        this.clientId = clientId;
        this.baseMagazineStorage = baseMagazineStorage;
        this.magazineIdentifier = magazineIdentifier;
    }

    public boolean load(final T data) {
        return baseMagazineStorage.load(magazineIdentifier, data);
    }

    public boolean reload(final T data) {
        return baseMagazineStorage.reload(magazineIdentifier, data);
    }

    public Optional<T> fire() {
        return baseMagazineStorage.fire(magazineIdentifier);
    }

    public MetaData getMetaData() {
        return baseMagazineStorage.getMetaData(magazineIdentifier);
    }
}
