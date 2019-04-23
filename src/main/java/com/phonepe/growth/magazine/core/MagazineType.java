package com.phonepe.growth.magazine.core;

import lombok.Getter;

public enum MagazineType {
    AEROSPIKE(MagazineType.AEROSPIKE_TEXT),
    HBASE(MagazineType.HBASE_TEXT);

    static final String AEROSPIKE_TEXT = "AEROSPIKE";
    static final String HBASE_TEXT = "HBASE";

    @Getter
    private final String value;

    MagazineType(String value) {
        this.value = value;
    }
}
