package com.phonepe.growth.magazine.core;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.phonepe.growth.magazine.impl.aerospike.AerospikeMagazine;
import com.phonepe.growth.magazine.impl.hbase.HBaseMagazine;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AerospikeMagazine.class, name = MagazineType.AEROSPIKE_TEXT),
        @JsonSubTypes.Type(value = HBaseMagazine.class, name = MagazineType.HBASE_TEXT), })
public abstract class BaseMagazine implements IMagazine {
    private final MagazineType type;

    public BaseMagazine(MagazineType type) {
        this.type = type;
    }
}
