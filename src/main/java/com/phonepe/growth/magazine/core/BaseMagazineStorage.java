package com.phonepe.growth.magazine.core;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.impl.aerospike.AerospikeStorage;
import com.phonepe.growth.magazine.impl.hbase.HBaseStorage;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Optional;

@Data
@EqualsAndHashCode
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AerospikeStorage.class, name = StorageType.AEROSPIKE_TEXT),
        @JsonSubTypes.Type(value = HBaseStorage.class, name = StorageType.HBASE_TEXT), })
public abstract class BaseMagazineStorage<T> {
    private final StorageType type;
    private final int recordTtl;

    public BaseMagazineStorage(StorageType type, int recordTtl) {
        this.type = type;
        this.recordTtl = recordTtl;
    }

    public abstract boolean load(String magazineIdentifier, T data);

    public abstract boolean reload(String magazineIdentifier, T data);

    public abstract Optional<T> fire(String magazineIdentifier);

    public abstract MetaData getMetaData(String magazineIdentifier);
}
