package com.phonepe.growth.magazine.core;

import com.phonepe.growth.magazine.common.Constants;
import com.phonepe.growth.magazine.common.MagazineData;
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
    private final int metaDataTtl;
    private final boolean enableDeDupe;
    private final int shards;
    private final String farmId;
    private final String clientId;

    public BaseMagazineStorage(
            final StorageType type,
            final int recordTtl,
            final int metaDataTtl,
            final String farmId,
            final boolean enableDeDupe,
            final int shards,
            final String clientId) {
        this.type = type;
        this.recordTtl = recordTtl;
        this.metaDataTtl = metaDataTtl;
        this.enableDeDupe = enableDeDupe;
        this.farmId = farmId;
        this.shards = shards < 1 ? Constants.MIN_SHARDS : shards;
        this.clientId = clientId;
    }

    public abstract boolean load(final String magazineIdentifier, final T data);

    public abstract boolean reload(final String magazineIdentifier, final T data);

    public abstract MagazineData<T> fire(final String magazineIdentifier);

    public abstract Map<String, MetaData> getMetaData(final String magazineIdentifier);

    public abstract void delete(final MagazineData<T> magazineData);
}
