package com.phonepe.growth.magazine.util;

/**
 * @author shantanu.tiwari
 */
public class ErrorMessages {
    public static final String ERROR_LOADING_DATA = "Error loading data [magazineIdentifier = %s]";
    public static final String ERROR_GETTING_META_DATA = "Error getting meta data [magazineIdentifier = %s]";
    public static final String ERROR_FIRING_DATA = "Error firing data [magazineIdentifier = %s]";
    public static final String ERROR_READING_POINTERS = "Error reading pointers [magazineIdentifier = %s]";
    public static final String ERROR_READING_COUNTERS = "Error reading counters [magazineIdentifier = %s]";
    public static final String NO_DATA_TO_FIRE = "No data to fire [magazineIdentifier = %s]";

    private ErrorMessages() {
        throw new IllegalStateException("Instantiation of this class is not allowed.");
    }
}
