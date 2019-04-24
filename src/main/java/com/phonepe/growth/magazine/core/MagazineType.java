package com.phonepe.growth.magazine.core;

import lombok.Getter;

public enum MagazineType {
    AEROSPIKE(MagazineType.AEROSPIKE_TEXT),
    HBASE(MagazineType.HBASE_TEXT);

    public static final String AEROSPIKE_TEXT = "AEROSPIKE";
    public static final String HBASE_TEXT = "HBASE";

    @Getter
    private final String value;

    MagazineType(String value) {
        this.value = value;
    }
}
