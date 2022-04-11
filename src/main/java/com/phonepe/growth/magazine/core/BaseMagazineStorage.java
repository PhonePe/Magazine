package com.phonepe.growth.magazine.core;

import com.phonepe.growth.magazine.common.Constants;
import com.phonepe.growth.magazine.common.FiredData;
import com.phonepe.growth.magazine.common.MetaData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;

@Data
@EqualsAndHashCode
@ToString
public abstract class BaseMagazineStorage<T> {
    private final StorageType type;
    private final int recordTtl;
    private final boolean enableDeDupe;
    private final int shards;

    public BaseMagazineStorage(final StorageType type, final int recordTtl, final boolean enableDeDupe, final int shards) {
        this.type = type;
        this.recordTtl = recordTtl;
        this.enableDeDupe = enableDeDupe;
        this.shards = shards < 1 ? Constants.MIN_SHARDS : shards;
    }

    public abstract boolean load(final String magazineIdentifier, final T data);

    public abstract boolean reload(final String magazineIdentifier, final T data);

    public abstract FiredData<T> fire(final String magazineIdentifier);

    public abstract Map<String, MetaData> getMetaData(final String magazineIdentifier);

    public abstract void delete(final FiredData<T> firedData);
}
