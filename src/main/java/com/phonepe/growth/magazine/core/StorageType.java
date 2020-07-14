package com.phonepe.growth.magazine.core;

import com.github.rholder.retry.RetryException;
import lombok.Getter;

import java.util.concurrent.ExecutionException;

public enum StorageType {
    AEROSPIKE(StorageType.AEROSPIKE_TEXT) {
        @Override
        public <T> T accept(StorageTypeVisitor<T> visitor) throws ExecutionException, RetryException {
            return visitor.visitAerospike();
        }
    },
    HBASE(StorageType.HBASE_TEXT) {
        @Override
        public <T> T accept(StorageTypeVisitor<T> visitor) throws ExecutionException, RetryException {
            return visitor.visitHBase();
        }
    };

    public static final String AEROSPIKE_TEXT = "AEROSPIKE";
    public static final String HBASE_TEXT = "HBASE";

    @Getter
    private final String value;

    StorageType(String value) {
        this.value = value;
    }

    public abstract <T> T accept(StorageTypeVisitor<T> visitor) throws ExecutionException, RetryException;
}
