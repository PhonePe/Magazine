package com.phonepe.growth.magazine.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author shantanu.tiwari
 */

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ErrorMessage {
    public static final String ERROR_LOADING_DATA = "Error loading data [magazineIdentifier = %s]";
    public static final String ERROR_GETTING_META_DATA = "Error getting meta data [magazineIdentifier = %s]";
    public static final String ERROR_FIRING_DATA = "Error firing data [magazineIdentifier = %s]";
    public static final String ERROR_READING_POINTERS = "Error reading pointers [magazineIdentifier = %s]";
    public static final String ERROR_READING_COUNTERS = "Error reading counters [magazineIdentifier = %s]";
    public static final String NO_DATA_TO_FIRE = "No data to fire [magazineIdentifier = %s]";
    public static final String CLASS_NOT_SUPPORTED_FOR_DEDUPE = "Class %s not supported for dedupe";
}
