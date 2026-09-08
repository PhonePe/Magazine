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

package com.phonepe.magazine.exception;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Factories for the exceptions raised across the library. Centralised so that a given
 * {@link ErrorCode} is always paired with a consistent message shape.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MagazineExceptions {

    public static MagazineException invalidConfiguration(final String message) {
        return of(ErrorCode.INVALID_CONFIGURATION, message);
    }

    public static MagazineException invalidShards(final String message) {
        return of(ErrorCode.INVALID_SHARDS, message);
    }

    public static MagazineException magazineUnprepared(final String message) {
        return of(ErrorCode.MAGAZINE_UNPREPARED, message);
    }

    public static MagazineException nothingToFire(final String message) {
        return of(ErrorCode.NOTHING_TO_FIRE, message);
    }

    public static MagazineException retriesExhausted(final String message) {
        return of(ErrorCode.RETRIES_EXHAUSTED, message);
    }

    public static MagazineException retriesExhausted(final String message, final Throwable cause) {
        return of(ErrorCode.RETRIES_EXHAUSTED, message, cause);
    }

    public static MagazineException connectionError(final String message, final Throwable cause) {
        return of(ErrorCode.CONNECTION_ERROR, message, cause);
    }

    public static MagazineException dataTypeMismatch(final String message) {
        return of(ErrorCode.DATA_TYPE_MISMATCH, message);
    }

    public static MagazineException of(final ErrorCode errorCode, final String message) {
        return of(errorCode, message, null);
    }

    public static MagazineException of(final ErrorCode errorCode,
            final String message,
            final Throwable cause) {
        return MagazineException.builder()
                .errorCode(errorCode)
                .message(message)
                .cause(cause)
                .build();
    }
}
