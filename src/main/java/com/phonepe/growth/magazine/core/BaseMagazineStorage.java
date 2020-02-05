package com.phonepe.growth.magazine.core;

import com.phonepe.growth.magazine.common.Constants;
import com.phonepe.growth.magazine.common.MetaData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;
import java.util.Optional;

@Data
@EqualsAndHashCode
@ToString
public abstract class BaseMagazineStorage<T> {
    private final StorageType type;
    private final int recordTtl;
    private final boolean enableDeDupe;
    private final int shards;

    public BaseMagazineStorage(StorageType type, int recordTtl, boolean enableDeDupe, int shards) {
        this.type = type;
        this.recordTtl = recordTtl;
        this.enableDeDupe = enableDeDupe;
        this.shards = shards < 1 ? Constants.MIN_SHARDS : shards;
    }

    public abstract boolean load(String magazineIdentifier, T data);

    public abstract boolean reload(String magazineIdentifier, T data);

    public abstract Optional<T> fire(String magazineIdentifier);

    public abstract Map<String, MetaData> getMetaData(String magazineIdentifier);
}
