package com.phonepe.growth.magazine.common;

public class Constants {

    private Constants() {
        throw new IllegalStateException("Instantiation of this class is not allowed.");
    }

    public static final String DATA = "data";
    public static final String MODIFIED_AT = "modified_at";
    public static final String KEY_DELIMITER = "_";

    public static final String POINTERS = "POINTERS";
    public static final String COUNTERS = "COUNTERS";
    public static final String LOAD_COUNTER = "LOAD_COUNTER";
    public static final String FIRE_COUNTER = "FIRE_COUNTER";
    public static final String LOAD_POINTER = "LOAD_POINTER";
    public static final String FIRE_POINTER = "FIRE_POINTER";
    public static final String SHARD_PREFIX = "SHARD_";

    public static final int DEFAULT_REFRESH = 5;
    public static final int DEFAULT_MAX_ELEMENTS = 1024;
    public static final int MAX_RETRIES = 5;
    public static final long DELAY_BETWEEN_RETRIES = 10; //in milliseconds
    public static final int MIN_SHARDS = 1;

}
