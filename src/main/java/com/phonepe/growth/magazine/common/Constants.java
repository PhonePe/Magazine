package com.phonepe.growth.magazine.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {
    public static final String DATA = "data";
    public static final String MODIFIED_AT = "modified_at";

    public static final String POINTERS = "POINTERS";
    public static final String COUNTERS = "COUNTERS";

    public static final String LOAD_COUNTER = "LOAD_COUNTER";
    public static final String FIRE_COUNTER = "FIRE_COUNTER";
    public static final String LOAD_POINTER = "LOAD_POINTER";
    public static final String FIRE_POINTER = "FIRE_POINTER";

    public static final String KEY_DELIMITER = "_";

    public static final int MAX_RETRIES = 5;
    public static final long DELAY_BETWEEN_RETRIES = 10; //in milliseconds
}
