/**
 * Copyright (c) 2025 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
