package com.phonepe.growth.magazine.core;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
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
        @JsonSubTypes.Type(value = AerospikeStorage.class, name = MagazineType.AEROSPIKE_TEXT),
        @JsonSubTypes.Type(value = HBaseStorage.class, name = MagazineType.HBASE_TEXT), })
public abstract class BaseMagazineStorage {
    private final MagazineType type;

    public BaseMagazineStorage(MagazineType type) {
        this.type = type;
    }

    public abstract boolean prepare(String keyPrefix);

    public abstract boolean load(String keyPrefix, Object data);

    public abstract Optional<Object> fire(String keyPrefix);
}
