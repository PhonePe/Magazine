package com.phonepe.growth.magazine.core;

import com.phonepe.growth.magazine.common.MetaData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Optional;

@Data
@EqualsAndHashCode
@ToString
public abstract class BaseMagazineStorage<T> {
    private final StorageType type;
    private final int recordTtl;
    private final boolean enableDeDupe;

    public BaseMagazineStorage(final StorageType type, final int recordTtl, final boolean enableDeDupe) {
        this.type = type;
        this.recordTtl = recordTtl;
        this.enableDeDupe = enableDeDupe;
    }

    public abstract boolean load(final String magazineIdentifier, final T data);

    public abstract boolean reload(final String magazineIdentifier, final T data);

    public abstract Optional<T> fire(final String magazineIdentifier);

    public abstract MetaData getMetaData(final String magazineIdentifier);
}
