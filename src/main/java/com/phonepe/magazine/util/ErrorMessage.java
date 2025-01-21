package com.phonepe.magazine.util;

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
    public static final String ERROR_DELETING_DATA = "Error deleting data [magazineIdentifier = %s]";
    public static final String ERROR_PEEKING_DATA = "Error peeking data [magazineIdentifier = %s]";
}
