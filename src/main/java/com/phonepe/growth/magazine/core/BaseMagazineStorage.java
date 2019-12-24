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
    private final boolean deDupeEnabled;

    public BaseMagazineStorage(StorageType type, int recordTtl, boolean deDupeEnabled) {
        this.type = type;
        this.recordTtl = recordTtl;
        this.deDupeEnabled = deDupeEnabled;
    }

    public abstract boolean load(String magazineIdentifier, T data);

    public abstract boolean reload(String magazineIdentifier, T data);

    public abstract Optional<T> fire(String magazineIdentifier);

    public abstract MetaData getMetaData(String magazineIdentifier);
}
