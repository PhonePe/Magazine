package com.phonepe.growth.magazine.core;

import lombok.Getter;

public enum StorageType {
    AEROSPIKE(StorageType.AEROSPIKE_TEXT),
    HBASE(StorageType.HBASE_TEXT);

    public static final String AEROSPIKE_TEXT = "AEROSPIKE";
    public static final String HBASE_TEXT = "HBASE";

    @Getter
    private final String value;

    StorageType(String value) {
        this.value = value;
    }
}
